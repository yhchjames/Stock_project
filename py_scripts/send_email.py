import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from collections import deque


# === 設定參數 ===
# csv_path = '/home/ec2-user/Stock_project/py_scripts/sh_logs/output_20250208_043001.log'          # CSV 檔案路徑
recipient = "yhchjames@gmail.com"          # 收件人信箱
sender = "yhchjames@gmail.com"                 # 寄件人信箱 (必須是你 SMTP 帳號)
smtp_server = "smtp.gmail.com"              # SMTP 伺服器（例如 AWS SES、Gmail 的 smtp.gmail.com 等）
# smtp_port = 587                              # SMTP 埠號（一般為 587 或 465）
smtp_port = 465                              # SMTP 埠號（一般為 587 或 465）
smtp_user = "yhchjames@gmail.com"                  # SMTP 帳號
smtp_password = "rspm npap pznj nxcs"              # SMTP 密碼或 App 密碼

import os
import glob
from collections import deque
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def get_latest_csv(directory):
    """
    在指定的目錄中尋找最新修改的 CSV 檔案
    """
    directory = os.path.expanduser(directory)
    csv_files = glob.glob(os.path.join(directory, '*.log'))
    if not csv_files:
        raise Exception("資料夾中找不到 CSV 檔案")
    # 使用檔案最後修改時間來判斷哪個檔案最新
    latest_file = max(csv_files, key=os.path.getmtime)
    return latest_file

# === 設定參數 ===
csv_directory = '~/Documents/Dev/Cheater_finder/Stock_project/py_scripts/sh_logs'   # CSV 檔案所在的資料夾
try:
    latest_csv = get_latest_csv(csv_directory)
    print(f"找到最新的 CSV 檔案：{latest_csv}")
except Exception as e:
    print(f"錯誤：{e}")
    exit(1)


# === 讀取 CSV 檔最後 10 行 ===
try:
    with open(latest_csv, 'r') as f:
        # 利用 deque 儲存最後 10 行
        last10 = deque(f, 10)
except Exception as e:
    print(f"讀取 CSV 檔案失敗: {e}")
    exit(1)

body = "".join(last10)

# === 建立郵件內容 ===
msg = MIMEMultipart()
msg['From'] = sender
msg['To'] = recipient
msg['Subject'] = "Cheater finding result from mac"

msg.attach(MIMEText(body, 'plain'))

# === 透過 SMTP 發送郵件 ===
try:
    server = smtplib.SMTP_SSL(smtp_server, port=smtp_port)
    # server.starttls()  # 如果 SMTP 伺服器支援 TLS
    server.login(smtp_user, smtp_password)
    server.send_message(msg)
    server.quit()
    print("郵件已成功發送！")
except Exception as e:
    print(f"發送郵件失敗: {e}")