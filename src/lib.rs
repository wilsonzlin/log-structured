use async_trait::async_trait;
use seekable_async_file::SeekableAsyncFile;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::join;
use tokio::sync::Mutex;
use tokio::time::sleep;
use write_journal::WriteJournal;

const STATE_OFFSETOF_HEAD: u64 = 0;
const STATE_OFFSETOF_TAIL: u64 = STATE_OFFSETOF_HEAD + 8;
pub const STATE_SIZE: u64 = STATE_OFFSETOF_TAIL + 8;

#[derive(Default)]
struct LogState {
  head: u64,
  tail: u64,
  // This is to prevent the scenario where a write at a later offset (i.e. subsequent request B) finishes before a write at an earlier offset (i.e. earlier request A); we can't immediately update the tail on disk after writing B because it would include A, which hasn't been synced yet.
  pending_tail_bumps: BTreeMap<u64, Option<SignalFutureController>>,
  // Necessary for GC to know where to safely read up to. `tail` may point past pending/partially-written data.
  tail_on_disk: u64,
}

#[derive(Clone, Copy)]
pub struct TailBump {
  pub acquired_physical_offset: u64,
  pub uncommitted_virtual_offset: u64,
}

pub enum GarbageCheck {
  IsGarbage(u64),
  IsNotGarbage,
  IsPadding,
}

#[async_trait]
pub trait GarbageChecker {
  async fn check_offset(&self, offset: u64) -> GarbageCheck;
}

pub struct LogStructured<GC: GarbageChecker> {
  device_offset: u64,
  device_size: u64,
  device: SeekableAsyncFile,
  free_space_gauge: Arc<AtomicU64>,
  garbage_checker: GC,
  journal: Arc<WriteJournal>,
  log_state: Mutex<LogState>,
  padding_indicator: Vec<u8>,
}

impl<GC: GarbageChecker> LogStructured<GC> {
  pub fn new(
    device: SeekableAsyncFile,
    device_offset: u64,
    device_size: u64,
    journal: Arc<WriteJournal>,
    garbage_checker: GC,
    padding_indicator: Vec<u8>,
    free_space_gauge: Arc<AtomicU64>,
  ) -> Self {
    Self {
      device_offset,
      device_size,
      device,
      free_space_gauge,
      garbage_checker,
      journal,
      log_state: Mutex::new(LogState::default()),
      padding_indicator,
    }
  }

  fn reserved_size(&self) -> u64 {
    self.device_offset + STATE_SIZE
  }

  pub fn physical_offset(&self, virtual_offset: u64) -> u64 {
    self.reserved_size() + (virtual_offset % (self.device_size - self.reserved_size()))
  }

  // How to use: bump tail, perform the write to the acquired tail offset, then persist the bumped tail. If tail is committed before write is persisted, it'll point to invalid data if the write didn't complete.
  pub async fn bump_tail(&self, usage: usize) -> TailBump {
    let usage: u64 = usage.try_into().unwrap();
    assert!(usage > 0);
    if usage > self.device_size - self.device_offset {
      panic!("out of storage space");
    };

    let (physical_offset, new_tail, write_filler_at) = {
      let mut state = self.log_state.lock().await;
      let mut physical_offset = self.physical_offset(state.tail);
      let mut write_filler_at = None;
      if physical_offset + usage >= self.device_size {
        // Write after releasing lock (performance) and checking tail >= head (safety).
        write_filler_at = Some(physical_offset);
        let filler = self.device_size - physical_offset;
        physical_offset = self.reserved_size();
        state.tail += filler;
      };

      state.tail += usage;
      let new_tail = state.tail;
      if new_tail - state.head > self.device_size - self.reserved_size() {
        panic!("out of storage space");
      };

      let None = state.pending_tail_bumps.insert(new_tail, None) else {
        unreachable!();
      };
      self
        .free_space_gauge
        .store(state.tail - state.head, Ordering::Relaxed);
      (physical_offset, new_tail, write_filler_at)
    };

    if let Some(write_filler_at) = write_filler_at {
      // TODO Prove safety.
      self
        .device
        .write_at(write_filler_at, self.padding_indicator.clone())
        .await;
    };

    TailBump {
      acquired_physical_offset: physical_offset,
      uncommitted_virtual_offset: new_tail,
    }
  }

  pub async fn commit_tail_bump(&self, bump: TailBump) {
    let (fut, fut_ctl) = SignalFuture::new();

    {
      let mut state = self.log_state.lock().await;

      *state
        .pending_tail_bumps
        .get_mut(&bump.uncommitted_virtual_offset)
        .unwrap() = Some(fut_ctl.clone());
    };

    fut.await;
  }

  async fn start_background_garbage_collection_loop(&self) {
    loop {
      sleep(std::time::Duration::from_secs(10)).await;

      let (orig_head, tail) = {
        let state = self.log_state.lock().await;
        (state.head, state.tail_on_disk)
      };
      let mut head = orig_head;
      // SAFETY:
      // - `head` is only ever modified by us so it's always what we expect.
      // - `tail` can be modified by others at any time but it only ever increases. If it physically reaches `head` (i.e. out of space), we panic.
      // - Data is never erased; only we can move the `head` to mark areas as free again but even then no data is written/cleared.
      // - Written log entries are never mutated/written to again, so we don't have to worry about other writers.
      // - Therefore, it's always safe to read from `head` to `tail`.
      while head < tail {
        let physical_offset = self.physical_offset(head);
        let res = self.garbage_checker.check_offset(physical_offset).await;
        match res {
          GarbageCheck::IsGarbage(ent_size) => {
            head += ent_size;
          }
          GarbageCheck::IsNotGarbage => {
            break;
          }
          GarbageCheck::IsPadding => {
            head += self.device_size - physical_offset;
          }
        };
      }
      if head != orig_head {
        self
          .journal
          .write(STATE_OFFSETOF_HEAD, head.to_be_bytes().to_vec())
          .await;
        let mut state = self.log_state.lock().await;
        state.head = head;
        self
          .free_space_gauge
          .store(state.tail - state.head, Ordering::Relaxed);
      };
    }
  }

  async fn start_background_tail_bump_commit_loop(&self) {
    loop {
      sleep(std::time::Duration::from_micros(200)).await;

      let mut to_resolve = vec![];
      let mut new_tail_to_write = None;
      {
        let mut state = self.log_state.lock().await;
        loop {
          let Some(e) = state.pending_tail_bumps.first_entry() else {
            break;
          };
          if e.get().is_none() {
            break;
          };
          let (k, fut_state) = e.remove_entry();
          to_resolve.push(fut_state.unwrap());
          new_tail_to_write = Some(k);
        }
        if let Some(tail) = new_tail_to_write {
          state.tail_on_disk = tail;
        };
      };

      if let Some(new_tail_to_write) = new_tail_to_write {
        self
          .journal
          .write(
            STATE_OFFSETOF_TAIL,
            new_tail_to_write.to_be_bytes().to_vec(),
          )
          .await;

        for ft in to_resolve {
          ft.signal();
        }
      };
    }
  }

  pub async fn start_background_loops(&self) {
    join! {
      self.start_background_garbage_collection_loop(),
      self.start_background_tail_bump_commit_loop(),
    };
  }

  pub async fn format_device(&self) {
    self
      .device
      .write_at(
        self.device_offset + STATE_OFFSETOF_HEAD,
        0u64.to_be_bytes().to_vec(),
      )
      .await;
    self
      .device
      .write_at(
        self.device_offset + STATE_OFFSETOF_TAIL,
        0u64.to_be_bytes().to_vec(),
      )
      .await;
  }

  pub async fn get_head_and_tail(&self) -> (u64, u64) {
    let log_state = self.log_state.lock().await;
    (log_state.head, log_state.tail)
  }

  pub async fn load_state_from_device(&self) {
    let head = self
      .device
      .read_u64_at(self.device_offset + STATE_OFFSETOF_HEAD)
      .await;
    let tail = self
      .device
      .read_u64_at(self.device_offset + STATE_OFFSETOF_TAIL)
      .await;
    self.free_space_gauge.store(tail - head, Ordering::Relaxed);
    {
      let mut log_state = self.log_state.lock().await;
      log_state.head = head;
      log_state.tail = tail;
    };
  }
}
