import rclpy
from rclpy.node import Node
from std_msgs.msg import Float32MultiArray
from pylsl import StreamInlet, resolve_streams
import threading

class EmotiBitNode(Node):
    def __init__(self):
        super().__init__('emotibit_lsl_node')
        self.publishers_ = {}
        self.get_logger().info('EmotiBit LSL Node 시작, 스트림 탐색 중...')
        threading.Thread(target=self.find_and_subscribe, daemon=True).start()

    def find_and_subscribe(self):
        streams = resolve_streams()
        self.get_logger().info(f'{len(streams)}개 LSL 스트림 발견')
        for stream in streams:
            name = stream.name()
            topic = f'/emotibit/{name.lower()}'
            self.publishers_[name] = self.create_publisher(Float32MultiArray, topic, 10)
            self.get_logger().info(f'토픽 생성: {topic}')
            threading.Thread(target=self.stream_data, args=(stream, name), daemon=True).start()

    def stream_data(self, stream_info, name):
        inlet = StreamInlet(stream_info)
        while rclpy.ok():
            sample, timestamp = inlet.pull_sample()
            msg = Float32MultiArray()
            msg.data = [float(x) for x in sample]
            self.publishers_[name].publish(msg)

def main(args=None):
    rclpy.init(args=args)
    node = EmotiBitNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()

if __name__ == '__main__':
    main()
