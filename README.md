# 📈 IPO Command Center — your own private copy

Track IPO applications across your whole family's demat accounts: apply checklist,
UPI rotation tracking, automatic allotment detection (KFin / MUFG Intime / Bigshare),
verified GMP & subscription data, listing-day CMP and per-account P&L — with phone alerts.

**Your data stays in YOUR copy.** Each person runs their own private instance with
their own passcode. Nobody else (including whoever shared this with you) can see it.

---

## 🚀 One-click setup (about 10 minutes, ₹0 cost)

### Step 1 — Create a free Render account
Go to **https://dashboard.render.com/register** and sign up with any email
(no credit card needed; "Free" plan is automatic).

### Step 2 — Deploy your copy
Click this button:

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/lelakolli/ipo-app)

Render shows a screen titled **"You are deploying ipo-app"** → press **Deploy Blueprint**.
It will ask for 3 values (fill all three, then deploy):

| Field | What to paste |
|---|---|
| `PASSCODE` | **Invent your own 6-digit number** (this is YOUR lock — do not use the sender's) |
| `GITHUB_TOKEN` | Paste the backup key your friend gave you (starts with `ghp_`) |
| `GITHUB_REPO` | Paste exactly: `lelakolli/ipo-friend-backup` |

After ~3 minutes your app is live at a URL like
`https://ipo-command-center-xxxx.onrender.com` — open it, enter your passcode.

*(If you skip the two GITHUB values, the app still works — but its data is wiped on
restarts. With them, it auto-saves and self-restores after every change.)*

### Step 3 — Keep it awake (free)
1. Go to **https://uptimerobot.com** → free sign up → **Add New Monitor**
2. Type: **HTTP(s)**  •  URL: `https://<your-app>.onrender.com/healthz`  •  Interval: **5 min**
3. Save. This pings the app so it never sleeps. (750 free Render hours = one full month 24×7.)

### Step 4 — First run in the app
1. **Accounts tab** → add your family accounts (name + PAN + CDSL BO ID + default UPI)
2. **IPOs tab** → 🔄 Sync live IPO list — the pipeline loads itself
3. For each IPO: open **Applications**, tick the accounts you applied with, fill UPI used
4. Allotment day → results are **checked automatically every 30 min**; nothing to do
5. **🔔 Enable phone alerts** in the alerts drawer for allotment/mandate/listing pushes

---

## Tips
- **Download backup** (Dashboard) occasionally — one tap, keeps a copy on your phone.
- Same app on other phones? Open your URL there and enter the same passcode.
- Something looks broken after a site changes its layout? Allotment falls back to
  "manual/review" and self-heals when patched upstream — you lose nothing.

## What this is not
- It never applies for you, never sells, never touches your money — you apply in your
  broker app as usual; this is your tracker and command center.
- It needs only PAN / CDSL BO ID (public registrar data) — never broker passwords.
