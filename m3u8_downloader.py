import os
import queue
import subprocess
from concurrent.futures import ThreadPoolExecutor
import threading
import m3u8
import requests
from Crypto.Cipher import AES


class M3u8DownLoader:

    def __init__(self, m3u8_url, base_path, base_url="", download_thread_number=50):
        self.FFMPEG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin/ffmpeg")
        self.m3u8_url = m3u8_url
        self.base_url = base_url
        self.base_path = base_path
        self.download_thread_number = download_thread_number
        self.segment_url = queue.Queue()
        self.segment_url_list = []
        self.key = None
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

    def __get_m3u8_key(self, m3u8_obj: m3u8.M3U8):
        if len(m3u8_obj.keys) > 0 and m3u8_obj.keys[0]:
            print("ts文件被加密过.")
            key = m3u8_obj.keys[0]
            key_abs_url = self.__find_absolute_uri(key)
            print("请求地址:%s" % key_abs_url)
            resp = requests.get(key_abs_url, timeout=15.0)
            print("key data:%s" % (resp.content.decode(encoding='utf-8')))
            return {"key": resp.content, "method": key.method}
        return None

    def parser_m3u8(self):
        print("解析m3u8文件")
        m3u8_obj = m3u8.load(self.m3u8_url)
        if m3u8_obj.playlists and len(m3u8_obj.playlists) > 0:
            self.m3u8_url = m3u8_obj.playlists[0].absolute_uri
            m3u8_obj = m3u8.load(self.m3u8_url)
        print(len(m3u8_obj.segments))
        self.key = self.__get_m3u8_key(m3u8_obj)
        print("key 结果为:%s" % self.key)
        for segment in m3u8_obj.segments:  # type:m3u8.Segment
            file_name = (segment.absolute_uri.rsplit("/")[-1]).rsplit("\\")[-1]
            download_url = self.__find_absolute_uri(segment=segment)
            item = {"file_name": file_name, "url": download_url}
            self.segment_url.put(item)
            self.segment_url_list.append(item)

    def __find_absolute_uri(self, segment):
        if segment.absolute_uri.startswith("http"):
            return segment.absolute_uri
        else:
            if self.base_url:
                return self.base_url + "/" + segment.uri
            return segment.absolute_uri

    def __download_segment(self):
        while self.segment_url.qsize() != 0:
            total_number = len(self.segment_url_list)
            downloaded_number = total_number - self.segment_url.qsize() - threading.active_count() + 1
            segment = self.segment_url.get()

            print("当前未下载文件个数:%d,完成率:%0.2f" % (self.segment_url.qsize(), downloaded_number / total_number))
            file_name = segment["file_name"]
            url = segment['url']
            save_path = os.path.join(self.base_path, file_name)
            save_path = save_path.split("?")[0]
            if os.path.exists(save_path):
                print("%s存在" % save_path)
                continue
            try:
                resp = requests.get(url=url, timeout=15.0)
                if resp.status_code != 200:
                    raise Exception('响应异常', resp.status_code)
                with open(save_path, 'wb') as f:
                    write_data = resp.content
                    if self.key:
                        print("根据key进行解密")
                        write_data = decrypt_by_aes(write_data, self.key)
                    f.write(write_data)
                    print("%s保存成功" % save_path)
            except Exception as e:
                print("%s下载失败" % url)
                print(e)
                self.segment_url.put(segment)
                if os.path.exists(save_path):
                    os.remove(save_path)

    def merge_video(self, out_file_name="result.mp4", delete=False):
        os.chdir(self.base_path)
        print("修改当前目录为:\n\t%s" % self.base_path)
        merge_txt = "merge.txt"
        with open(merge_txt, 'w') as f:
            for segment in self.segment_url_list:
                segment['file_name'] = segment['file_name'].split("?")[0]
                f.writelines("file '{file_name}'\n".format(file_name=segment['file_name']))
        cmd = self.FFMPEG_PATH + " -f concat -i {merge_file} -c copy output_{output_file} -y".format(
            merge_file=merge_txt,
            output_file=out_file_name)
        try:
            print("运行命令:\n\t%s" % cmd)
            res = subprocess.call(cmd, shell=True)
            if res != 0:
                print("合并文件出错")
                return
            print("合并文件成功")
            if delete and os.path.exists(merge_txt):
                os.remove(merge_txt)
            if delete:
                for item_file in self.segment_url_list:
                    print("删除文件:%s" % item_file['file_name'])
                    os.remove(item_file['file_name'])
        except Exception as e:
            print(e)
            print("合并文件出现异常")

    def thread_download(self):
        pool = ThreadPoolExecutor(self.download_thread_number)
        result = []
        for i in range(self.download_thread_number):
            result.append(pool.submit(self.__download_segment))
        for index, item in enumerate(result):
            item.result()
            print("%s号线程执行完毕" % str(index))


def decrypt_by_aes(encrypt_data: bytes, key: dict):
    if key["method"].startswith('AES'):
        print("根据aes解密 key:%s" % key['key'].decode(encoding='utf-8'))
        return AES.new(key=key['key'], mode=AES.MODE_CBC).decrypt(encrypt_data)


def sniff_cmd(cmd):
    res = subprocess.call(cmd, shell=True, stdout=None)
    if res != 0:
        return False
    return True


if __name__ == '__main__':
    loader = M3u8DownLoader(
        # http的m3u8路径
        m3u8_url="",
        base_path="./",
        download_thread_number=50,
    )
    loader.parser_m3u8()
    loader.thread_download()
    loader.merge_video(out_file_name="44.mp4", delete=True)
