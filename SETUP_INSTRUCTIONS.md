# Daily Note Generator - Setup Instructions

This system automatically fetches emails from specified senders and generates professional daily summaries.

## Prerequisites

- Python 3.7 or higher
- Gmail account
- Internet connection

## Step 1: Install Dependencies

Open a terminal in this directory and run:

```bash
pip install -r requirements.txt
```

## Step 2: Set Up Gmail App Password

Since Gmail requires secure authentication, you need to create an App Password:

1. **Enable 2-Factor Authentication** (if not already enabled):
   - Go to https://myaccount.google.com/security
   - Under "How you sign in to Google", select "2-Step Verification"
   - Follow the steps to enable it

2. **Generate App Password**:
   - Go to https://myaccount.google.com/apppasswords
   - Select "Mail" and "Windows Computer" (or Other)
   - Click "Generate"
   - Copy the 16-character password (you'll need this for config)

## Step 3: Configure Your Settings

1. Copy the template config file:
   ```bash
   copy config.json.template config.json
   ```

2. Edit `config.json` with your information:
   ```json
   {
     "email_address": "yourname@gmail.com",
     "password": "your-16-char-app-password",
     "imap_server": "imap.gmail.com",
     "target_senders": [
       "newsletter@techcrunch.com",
       "news@bloomberg.com",
       "updates@medium.com"
     ],
     "output_dir": "."
   }
   ```

   **Important**: Replace the example senders with the actual email addresses of newsletters/blogs you want to track.

## Step 4: Test the Script

Run the script manually to make sure it works:

```bash
python daily_note_generator.py
```

This will:
- Connect to your Gmail
- Fetch emails from the specified senders (from the last 24 hours)
- Generate a markdown file named `daily_note_YYYY-MM-DD.md`

## Step 5: Schedule Automatic Daily Execution

### On Windows (Task Scheduler):

1. **Open Task Scheduler**:
   - Press `Win + R`, type `taskschd.msc`, press Enter

2. **Create a New Task**:
   - Click "Create Basic Task" in the right panel
   - Name: "Daily Note Generator"
   - Description: "Generates daily news summary from emails"
   - Click "Next"

3. **Set Trigger**:
   - Select "Daily"
   - Set your preferred time (e.g., 8:00 AM)
   - Click "Next"

4. **Set Action**:
   - Select "Start a program"
   - Program/script: `python`
   - Add arguments: `daily_note_generator.py`
   - Start in: `C:\Users\daqui\PycharmProjects\PythonProject1`
   - Click "Next", then "Finish"

5. **Configure Additional Settings**:
   - Right-click your new task and select "Properties"
   - Under "General" tab, check "Run whether user is logged on or not"
   - Under "Conditions" tab, uncheck "Start the task only if the computer is on AC power"
   - Click "OK"

### On Mac/Linux (Cron):

1. Open terminal and edit crontab:
   ```bash
   crontab -e
   ```

2. Add this line (runs daily at 8:00 AM):
   ```
   0 8 * * * cd /path/to/project && /usr/bin/python3 daily_note_generator.py
   ```

3. Save and exit

## Output Format

The script generates a professional markdown file with:

- Date header
- Summary count of emails
- Organized by sender
- Each email includes:
  - Subject line
  - Received date/time
  - Summary (first 500 characters)
- Generation timestamp

Example filename: `daily_note_2024-03-15.md`

## Troubleshooting

### "Authentication failed" error:
- Make sure you're using an App Password, not your regular Gmail password
- Verify 2-Factor Authentication is enabled
- Check that the email address is correct

### "No emails found":
- Verify the sender addresses in `config.json` match exactly
- Check that you actually received emails from those senders in the last 24 hours
- Try increasing `days_back` parameter in the script

### "ModuleNotFoundError":
- Make sure you ran `pip install -r requirements.txt`
- Verify you're using the correct Python environment

### Scheduled task not running:
- Check Task Scheduler history to see error messages
- Make sure the paths in the task are absolute paths
- Test the script manually first to ensure it works

## Security Notes

- **Never commit `config.json`** to version control (it contains your password)
- The App Password only works with this app and can be revoked anytime
- Your email password is never exposed
- All connections use SSL/TLS encryption

## Customization

You can modify the script to:
- Change the summary length (line 161)
- Adjust the date range (`days_back` parameter)
- Customize the markdown format
- Add filtering by subject keywords
- Export to different formats (HTML, PDF, etc.)

## Support

If you encounter issues:
1. Check the troubleshooting section above
2. Verify your Python version: `python --version`
3. Test with a single sender first
4. Check Gmail settings allow IMAP access

Enjoy your automated daily news summaries!
