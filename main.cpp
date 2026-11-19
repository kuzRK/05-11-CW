#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <stdexcept>
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

  explicit th_pool_t(std::size_t k, std::size_t batch_size = 2)
      : queues_(k), batch_size_(batch_size)
  {
    if (k == 0 || batch_size == 0)
    {
      throw std::invalid_argument("Invalid pool size");
    }

    for (std::size_t i = 0; i < k; ++i)
    {
      ths_.emplace_back([this, i]()
                        { execute(i); });
    }
  }

  ~th_pool_t()
  {
    {
      std::lock_guard<std::mutex> lock(mx_);
      stop_ = true;
      paused_ = false;
    }

    cd_.notify_all();

    for (auto &th : ths_)
    {
      th.join();
    }
  }

  void submit(f_t f)
  {
    {
      std::lock_guard<std::mutex> lock(mx_);

      if (stop_)
      {
        throw std::runtime_error("Pool stopped");
      }

      queues_[next_queue_].push_back(std::move(f));
      next_queue_ = (next_queue_ + 1) % queues_.size();
      ++queued_;
    }

    cd_.notify_all();
  }

  void wait()
  {
    std::unique_lock<std::mutex> lock(mx_);

    done_cd_.wait(lock, [this]()
                  { return queued_ == 0 && active_ == 0; });
  }

  void pause()
  {
    std::lock_guard<std::mutex> lock(mx_);
    paused_ = true;
  }

  void resume()
  {
    {
      std::lock_guard<std::mutex> lock(mx_);
      paused_ = false;
    }

    cd_.notify_all();
  }

  std::size_t stolen_tasks()
  {
    std::lock_guard<std::mutex> lock(mx_);
    return stolen_tasks_;
  }

private:
  void execute(std::size_t id)
  {
    while (true)
    {
      std::vector<f_t> batch;

      {
        std::unique_lock<std::mutex> lock(mx_);

        cd_.wait(lock, [this]()
                 { return stop_ || (!paused_ && queued_ > 0); });

        if (stop_ && queued_ == 0)
        {
          return;
        }

        std::size_t owner = id;

        if (queues_[owner].empty())
        {
          std::size_t largest = 0;

          for (std::size_t i = 0; i < queues_.size(); ++i)
          {
            if (queues_[i].size() > largest)
            {
              largest = queues_[i].size();
              owner = i;
            }
          }

          if (largest == 0)
          {
            continue;
          }
        }

        std::size_t count =
            std::min(batch_size_, queues_[owner].size());

        for (std::size_t i = 0; i < count; ++i)
        {
          if (owner == id)
          {
            batch.push_back(
                std::move(queues_[owner].front()));
            queues_[owner].pop_front();
          }
          else
          {
            batch.push_back(
                std::move(queues_[owner].back()));
            queues_[owner].pop_back();
            ++stolen_tasks_;
          }
        }

        queued_ -= count;
        active_ += count;
      }

      for (auto &task : batch)
      {
        try
        {
          task();
        }
        catch (...)
        {
        }

        std::lock_guard<std::mutex> lock(mx_);
        --active_;

        if (queued_ == 0 && active_ == 0)
        {
          done_cd_.notify_all();
        }
      }
    }
  }

  std::vector<std::deque<f_t>> queues_;
  const std::size_t batch_size_;

  std::size_t next_queue_{0};
  std::size_t queued_{0};
  std::size_t active_{0};
  std::size_t stolen_tasks_{0};

  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable done_cd_;

  bool stop_{false};
  bool paused_{false};

  std::vector<std::thread> ths_;
};

int main()
{
  std::mutex cout_mx;
  th_pool_t pool(3, 2);

  pool.pause();

  for (int i = 0; i < 18; ++i)
  {
    int milliseconds = (i % 3 == 0) ? 500 : 40;

    pool.submit([i, milliseconds, &cout_mx]()
                {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(milliseconds)
            );

            std::lock_guard<std::mutex> lock(cout_mx);

            std::cout << "Task " << i
                      << " (" << milliseconds << " ms)"
                      << " on thread "
                      << std::this_thread::get_id()
                      << '\n'; });
  }

  pool.resume();
  pool.wait();

  std::cout << "Stolen tasks: "
            << pool.stolen_tasks() << '\n';

  std::cout << "All tasks finished\n";
}
