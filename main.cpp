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
  void execute();

  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable wait_cv_;
  bool stop_{false};
  std::vector< std::thread > ths_;
  size_t pending_tasks_{0}; 
};

template< class T >
auto th_pool_t::submit(T && t) -> std::future< decltype(t()) > {
  using return_type = decltype(t());
  auto task_ptr = std::make_shared< std::packaged_task< return_type() > >(std::forward< T >(t));
  std::future< return_type > res = task_ptr->get_future();

  {
    std::lock_guard< std::mutex > lck(mx_);
    tasks_.push([task_ptr]() {
      (*task_ptr)();
    });
    pending_tasks_++;
  }
  cd_.notify_one();
  return res;
}

void th_pool_t::wait_all() {
  std::unique_lock< std::mutex > lck(mx_);
  wait_cv_.wait(lck, [this]() { 
    return tasks_.empty() && pending_tasks_ == 0; 
  });
}

th_pool_t::th_pool_t(unsigned int k) {
  auto thf = [this]() {
    execute();
  };
  for (; k; --k) {
    ths_.emplace_back(thf);
  }
}

th_pool_t::~th_pool_t() {
  mx_.lock();
  stop_ = true;
  mx_.unlock();
  cd_.notify_all();
  for (auto && th : ths_) {
    th.join();
  }
}

void th_pool_t::execute() {
  std::unique_lock< std::mutex > lck(mx_);
  do {
    auto proceed = [&]() {
      return !tasks_.empty() || stop_;
    };
    cd_.wait(lck, proceed);
    if (tasks_.empty()) {
      continue;
    }
    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();
    try { 
      task(); 
    } catch (...) {
    }
    lck.lock();
    pending_tasks_--;
    if (pending_tasks_ == 0 && tasks_.empty()) {
      wait_cv_.notify_all();
    }
  } while (!stop_ || !tasks_.empty());
}

int main() {
  th_pool_t pool(4);
  auto future1 = pool.submit([]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return 42;
  });

  auto future2 = pool.submit([]() {
    std::cout << "[Thread] Hello from void task!\n";
  });

  auto future3 = pool.submit([]() -> int {
    throw std::runtime_error("Something went wrong inside the task!");
  });

  for (int i = 0; i < 5; ++i) {
    pool.submit([i]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      std::cout << "[Thread] Background task " << i << " done.\n";
    });
  }

  pool.wait_all(); 
  std::cout << future1.get() << '\n';
  future2.get(); 
  try {
    future3.get();
  } catch (const std::exception & e) {
    std::cout << "Caught exception from future3: " << e.what() << "\n";
  }

  return 0;
}
