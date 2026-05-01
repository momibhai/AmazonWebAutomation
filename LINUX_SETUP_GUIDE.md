# 🐧 Linux Setup Guide - Amazesst Amazon Automation

(Tested on Ubuntu/Debian based systems)

## Step 1: System Update & Python 3.10
Sabse pehle system update karein aur Python 3.10 install karein.

Terminal open karein (`Ctrl+Alt+T`) aur ye commands run karein:

```bash
# System Update
sudo apt update
sudo apt upgrade -y

# Install Python 3.10 and dependencies
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.10 python3.10-venv python3.10-dev python3-pip -y
```

Verify karein:
```bash
python3.10 --version
```

---

## Step 2: Install Google Chrome
Automation ke liye Chrome browser zaroori hai.

```bash
# Download latest Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

# Install Chrome
sudo apt install ./google-chrome-stable_current_amd64.deb -y
```

Agar koi error aaye, toh ye run karein: `sudo apt --fix-broken install -y`

---

## Step 3: Copy Project Folder
Apne Windows/Mac se **pura folder** copy karein Linux mein (USB ya Cloud se).
Example: Agar folder Desktop par hai.

```bash
cd ~/Desktop/AmazonWebAutomation
```

---

## Step 4: Create Virtual Environment
Project folder ke andar Virtual Environment banayein:

```bash
# Create venv
python3.10 -m venv venv

# Activate venv
source venv/bin/activate
```

Prompt ke shuru mein `(venv)` dikhna chahiye.

---

## Step 5: Install Dependencies
Libraries install karein:

```bash
pip install -r requirements.txt
```

---

## Step 6: Configuration Check
Make sure aapke paas ye 2 files hain:

1.  **credentials.json**: Client ki Google Service Account file.
2.  **config.json**: Sheet settings. Agar nahi hai toh create karein:

```bash
nano config.json
```
(Content paste karein, phir `Ctrl+O`, Enter, `Ctrl+X` se save karein)

---

## Step 7: Run Application
App chalayein:

```bash
streamlit run app.py
```

Browser open hoga `http://localhost:8501` par.

---

## 🔧 Troubleshooting

### "Module not found" Error?
Make sure `venv` activated hai (`source venv/bin/activate`) aur aapne `pip install -r requirements.txt` run kiya hai.

### Chrome Driver Issues?
Linux par `undetected-chromedriver` kabhi kabhi issue karta hai agar `xvfb` nahi hai (headless environments ke liye). Desktop Linux par zaroorat nahi honi chahiye, lekin agar error aaye toh:

```bash
sudo apt install xvfb -y
```

### Permission Denied?
Agar script permission error de:
```bash
chmod +x app.py
chmod +x venv/bin/activate
```

---

## 🛑 Stop Application
Stop karne ke liye Terminal mein `Ctrl+C` dabayein.
