# Record with: vhs examples/gauntlet/demo.tape   (brew install vhs)
Output examples/gauntlet/gauntlet.gif

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 720
Set Theme "Catppuccin Mocha"
Set TypingSpeed 40ms

Type ".venv/bin/python examples/gauntlet/gauntlet.py --tasks 6000 --disk-death"
Sleep 800ms
Enter
Sleep 24s
Screenshot examples/gauntlet/ledger.png
Sleep 3s
