import sched
import time
import datetime
import subprocess

def run_script():
    # 指定要运行的 .py 文件的路径
    script_path = './crawler.py'
    
    # 运行 .py 文件
    subprocess.run(['python', script_path])

def weekly_task(sc):
    # 计算下一次运行时间（一周后）
    next_run_time = datetime.datetime.now() + datetime.timedelta(weeks=1)
    
    # 输出下一次运行时间
    print(f"下一次运行时间: {next_run_time}")
    
    # 运行任务
    run_script()
    
    # 重新调度下一次任务
    sc.enter(604800, 1, weekly_task, (sc,))

def main():
    # 创建调度器
    s = sched.scheduler(time.time, time.sleep)
    
    # 第一次运行
    s.enter(0, 1, weekly_task, (s,))
    
    # 启动调度器
    s.run()

if __name__ == "__main__":
    main()
