# AMT Trade Journal

Automated trade journal and position viewer for [Sierra Chart](https://www.sierrachart.com/). Connects via DTC Protocol (WebSocket + JSON) and tracks every fill in a local SQLite database — zero manual entry.

![Python](https://img.shields.io/badge/Python-3.12+-blue) ![License](https://img.shields.io/badge/License-MIT-green) ![Platform](https://img.shields.io/badge/Platform-Windows-lightgrey)

## Features

- **Multi-instance support** — connect to unlimited Sierra Chart instances simultaneously
- **Automatic trade capture** — every fill recorded via DTC Protocol, no manual logging
- **Performance analytics** — win rate, P&L, R-multiples, drawdown, and more
- **Trade calendar** — visual heatmap of daily performance
- **Export** — SQLite database + CSV/ZIP export for external analysis
- **Dark theme** — purpose-built UI for extended screen time

## Quick Start

### Download (recommended)

Grab the latest `.exe` from [amtbalance.com/tools/journal](https://amtbalance.com/tools/journal) — no Python install required.

### Run from source

```bash
pip install -r requirements.txt
python sc_position_dashboard.pyw
```

## Requirements

- **Sierra Chart** with DTC Protocol enabled (port 11050 by default)
- **Windows 10/11**
- **Python 3.12+** (only if running from source)

## Configuration

On first launch the app creates `sc_instances.json` in the same directory. Edit this file or use the built-in Connection Manager (gear icon) to add/remove Sierra Chart instances.

## Building the exe

```bash
pip install pyinstaller
pyinstaller "AMT Trade Journal.spec"
```

The output lands in `dist/AMT Trade Journal.exe`.

## License

[MIT](LICENSE)

## Links

- [AMT Website](https://amtbalance.com)
- [Download Journal](https://amtbalance.com/tools/journal)
