#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <utility>
#include <future>
#include <memory>
#include <atomic>
#include <chrono>

struct task_t { 
  void operator()()
  {} 
};

class th_pool_t {
public:
  using f_t = std::function< void() >;

  explicit th_pool_t(unsigned int k);
  ~th_pool_t();

  template< class T >
  auto submit(T && t) -> std::future< decltype(t()) >;

  void wait_all();

private:
  void execute(size_t thread_id);
  bool try_steal(size_t thread_id, f_t & task_out);

  std::vector< std::queue< f_t > > queues_;
  std::vector< std::mutex > mutexes_;
  std::mutex wait_mx_;
  std::condition_variable wait_cv_;
  std::atomic< size_t > pending_tasks_{0};
  bool stop_{false};
  std::vector< std::thread > ths_;
  std::atomic< size_t > total_tasks_pushed_{0};
  std::atomic< size_t > total_tasks_completed_{0};
};

template< class T >
auto th_pool_t::submit(T && t) -> std::future< decltype(t()) > {
  using return_type = decltype(t());
  auto task_ptr = std::make_shared< std::packaged_task< return_type() > >(std::forward< T >(t));
  std::future< return_type > res = task_ptr->get_future();
  static std::atomic<size_t> rr_counter{0};
  size_t target_q = rr_counter++ % queues_.size();

  {
    std::lock_guard< std::mutex > lck(mutexes_[target_q]);
    queues_[target_q].push([task_ptr = std::move(task_ptr)]() {
      (*task_ptr)();
    });
    pending_tasks_++;
    total_tasks_pushed_++;
  }
  wait_cv_.notify_all();
  return res;
}

void th_pool_t::wait_all() {
  std::unique_lock< std::mutex > lck(wait_mx_);
  wait_cv_.wait(lck, [this]() { 
    return pending_tasks_ == 0; 
  });
}

th_pool_t::th_pool_t(unsigned int k) {
  queues_.resize(k);
  mutexes_ = std::vector< std::mutex >(k);
  ths_.reserve(k);
  for (unsigned int i = 0; i < k; ++i) {
    ths_.emplace_back([this, i]() {
      execute(i);
    });
  }
}

th_pool_t::~th_pool_t() {
  stop_ = true;
  wait_cv_.notify_all();
  for (auto & th : ths_) {
    th.join();
  }
}

void th_pool_t::execute(size_t thread_id) {
  while (!stop_) {
    f_t task;
    bool found = false;
    {
      std::lock_guard< std::mutex > lck(mutexes_[thread_id]);
      if (!queues_[thread_id].empty()) {
        task = std::move(queues_[thread_id].front());
        queues_[thread_id].pop();
        found = true;
      }
    }
    if (!found) {
      found = try_steal(thread_id, task);
    }
    if (found) {
      try {
        task();
      } catch (...) {
      }
      pending_tasks_--;
      total_tasks_completed_++;
      if (pending_tasks_ == 0) {
        wait_cv_.notify_all();
      }
    } else {
      std::this_thread::yield(); 
    }
  }
}

bool th_pool_t::try_steal(size_t thread_id, f_t & task_out) {
  size_t num_queues = queues_.size();
  for (size_t i = 1; i < num_queues; ++i) {
    size_t victim_id = (thread_id + i) % num_queues;
    std::lock_guard< std::mutex > lck(mutexes_[victim_id]);
    if (!queues_[victim_id].empty()) {
      task_out = std::move(queues_[victim_id].front());
      queues_[victim_id].pop();
      return true;
    }
  }
  return false;
}

int main() {
    std::cout << "=== Testing Work Stealing ===\n";
    th_pool_t pool(4);

    auto heavy = pool.submit([]() {
      std::cout << "[Heavy] Started on thread...\n";
      std::this_thread::sleep_for(std::chrono::seconds(2));
      std::cout << "[Heavy] Finished!\n";
      return 1;
    });

    for (int i = 0; i < 8; ++i) {
      pool.submit([i]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::cout << "[Fast] Task " << i << " done by thread " << std::this_thread::get_id() << "\n";
      });
    }

    pool.wait_all();
    std::cout << "Heavy result: " << heavy.get() << "\n";
    std::cout << "=== Done ===\n";
    return 0;
}
