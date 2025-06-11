import time
from collections import deque

# Cấu hình cho Sliding Window
WINDOW_SIZE = 5  # Kích thước cửa sổ (số lượng bản ghi trong cửa sổ)
TIME_WINDOW = 10  # Khoảng thời gian cửa sổ (tính bằng giây)

class SlidingWindowJoin:
    def __init__(self, window_size=WINDOW_SIZE, time_window=TIME_WINDOW):
        self.window_size = window_size
        self.time_window = time_window
        self.window_a = deque()  # Cửa sổ cho luồng A
        self.window_b = deque()  # Cửa sổ cho luồng B

    # Hàm để thêm dữ liệu vào cửa sổ
    def add_to_window(self, window, data):
        window.append(data)
        while window and window[0][0] < time.time() - self.time_window:
            window.popleft()

    # Hàm để thực hiện join giữa luồng A và B
    def join(self):
        results = []
        for a in self.window_a:
            for b in self.window_b:
                if a[0] == b[0]:  # Điều kiện join đơn giản (cùng thời gian)
                    results.append((a[1], b[1]))
        return results

    # Hàm xử lý luồng A
    def process_stream_a(self, data):
        self.add_to_window(self.window_a, data)
        return self.join()

    # Hàm xử lý luồng B
    def process_stream_b(self, data):
        self.add_to_window(self.window_b, data)
        return self.join()

# Khởi tạo SlidingWindowJoin
sw_join = SlidingWindowJoin()

# Mô phỏng dữ liệu đầu vào
stream_a = [(time.time(), f"A-{i}") for i in range(10)]  # Dữ liệu cho luồng A
stream_b = [(time.time(), f"B-{i}") for i in range(10)]  # Dữ liệu cho luồng B

# Xử lý luồng A và B
for i in range(10):
    print(f"Processing Stream A: {stream_a[i]}")
    result_a = sw_join.process_stream_a(stream_a[i])
    print(f"Join Results A: {result_a}")

    time.sleep(1)  # Giả lập độ trễ giữa các bản ghi

    print(f"Processing Stream B: {stream_b[i]}")
    result_b = sw_join.process_stream_b(stream_b[i])
    print(f"Join Results B: {result_b}")
