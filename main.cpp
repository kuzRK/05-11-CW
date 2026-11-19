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


struct th_pool_t {
    using f_t = std::function<void()>;

    explicit th_pool_t(std::size_t k, std::size_t batch_size = 2)
        : queues_(k), batch_size_(batch_size) {
        if (k == 0 || batch_size == 0) {
            throw std::invalid_argument("Invalid pool size");
        }

        for (std::size_t i = 0; i < k; ++i) {
            ths_.emplace_back([this, i]() { execute(i); });
        }
    }

    ~th_pool_t() {
        {
            std::lock_guard<std::mutex> lock(mx_);
            stop_ = true;
            paused_ = false;
        }

        cd_.notify_all();

        for (auto& th : ths_) {
            th.join();
        }
    }

    void submit(f_t f) {
        {
            std::lock_guard<std::mutex> lock(mx_);

            if (stop_) {
                throw std::runtime_error("Pool stopped");
            }

            queues_[next_queue_].push_back(std::move(f));
            next_queue_ = (next_queue_ + 1) % queues_.size();
            ++queued_;
        }

        cd_.notify_all();
    }

    void wait() {
        std::unique_lock<std::mutex> lock(mx_);

        done_cd_.wait(lock, [this]() {
            return queued_ == 0 && active_ == 0;
        });
    }

    void pause() {
        std::lock_guard<std::mutex> lock(mx_);
        paused_ = true;
    }

    void resume() {
        {
            std::lock_guard<std::mutex> lock(mx_);
            paused_ = false;
        }

        cd_.notify_all();
    }

private:
    void execute(std::size_t id) {
        while (true) {
            std::vector<f_t> batch;

            {
                std::unique_lock<std::mutex> lock(mx_);

                cd_.wait(lock, [this, id]() {
                    return stop_ ||
                        (!paused_ && !queues_[id].empty());
                });

                if (stop_ && queues_[id].empty()) {
                    return;
                }

                std::size_t count =
                    std::min(batch_size_, queues_[id].size());

                for (std::size_t i = 0; i < count; ++i) {
                    batch.push_back(
                        std::move(queues_[id].front())
                    );
                    queues_[id].pop_front();
                }

                queued_ -= count;
                active_ += count;
            }

            for (auto& task : batch) {
                try {
                    task();
                } catch (...) {}

                std::lock_guard<std::mutex> lock(mx_);
                --active_;

                if (queued_ == 0 && active_ == 0) {
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

    std::mutex mx_;
    std::condition_variable cd_;
    std::condition_variable done_cd_;

    bool stop_{false};
    bool paused_{false};

    std::vector<std::thread> ths_;
};

int main() {
    th_pool_t pool(3, 2);

    for (int i = 0; i < 6; ++i) {
        pool.submit([i]() {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100)
            );
        });
    }

    pool.wait();
    std::cout << "Finished\n";
}
