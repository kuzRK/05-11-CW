#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

struct task_t
{
  void operator()() const
  {
    using namespace std::chrono_literals;

    std::this_thread::sleep_for(200ms);

    static std::mutex cout_mx;
    std::lock_guard<std::mutex> lock(cout_mx);

    std::cout << "Task completed\n";
  }
};

struct th_pool_t
{
  using f_t = std::function<void()>;

  explicit th_pool_t(unsigned int k);
  ~th_pool_t();

  void submit(f_t f);
  void wait();
  void pause();
  void resume();

private:
  void execute();

  std::queue<f_t> tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable done_cd_;
  std::size_t active_{0};
  bool stop_{false};
  bool paused_{false};
  std::vector<std::thread> ths_;
};

th_pool_t::th_pool_t(unsigned int k)
{
  for (unsigned int i = 0; i < k; ++i)
  {
    ths_.emplace_back([this]()
                      { execute(); });
  }
}

th_pool_t::~th_pool_t()
{
  {
    std::lock_guard<std::mutex> lck(mx_);
    stop_ = true;
  }

  cd_.notify_all();

  for (auto &th : ths_)
  {
    th.join();
  }
}

void th_pool_t::submit(f_t f)
{
  {
    std::lock_guard<std::mutex> lck(mx_);
    tasks_.push(std::move(f));
  }

  cd_.notify_one();
}

void th_pool_t::execute()
{
  while (true)
  {
    std::unique_lock<std::mutex> lck(mx_);

    cd_.wait(lck, [this]()
             { return stop_ || (!paused_ && !tasks_.empty()); });

    if (stop_ && tasks_.empty())
    {
      return;
    }

    auto task = std::move(tasks_.front());
    tasks_.pop();

    ++active_;

    lck.unlock();

    try
    {
      task();
    }
    catch (...)
    {}

    lck.lock();
    --active_;

    if (tasks_.empty() && active_ == 0)
    {
      done_cd_.notify_all();
    }
  }
}

void th_pool_t::wait()
{
  std::unique_lock<std::mutex> lck(mx_);

  done_cd_.wait(lck, [this]()
                { return tasks_.empty() && active_ == 0; });
}

void th_pool_t::pause()
{
  std::lock_guard<std::mutex> lck(mx_);
  paused_ = true;
}

void th_pool_t::resume()
{
  {
    std::lock_guard<std::mutex> lck(mx_);
    paused_ = false;
  }

  cd_.notify_all();
}

int main() {
    th_pool_t pool(3);

    pool.pause();

    for (int i = 0; i < 6; ++i) {
        pool.submit(task_t{});
    }

    std::cout << "Pool paused.\n" << std::flush;

    std::this_thread::sleep_for(
        std::chrono::milliseconds(300)
    );

    std::cout << "Resuming...\n" << std::flush;

    pool.resume();

    pool.wait();

    std::cout << "All tasks finished.\n";
}
