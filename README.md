# DoughZone Analytics Dashboard

A data analytics dashboard for Dough Zone that visualizes sales, menu performance, inventory, and customer reviews.

## Project Structure

```
dashboard-app/
├── doughzone_dashboard.tsx    # Original React component (being converted)
├── data/                       # Restaurant data by location and date
│   └── [LOCATION_ID]/
│       └── [YYYYMMDD]/
│           ├── *.csv          # Sales, order, inventory, timing data
│           └── *.xlsx         # Reports and detailed data
├── docs/                       # Project documentation
│   ├── CLAUDE.md               # Development guide for Claude Code
│   └── ARCHITECTURE.md         # Architecture overview
├── README.md                   # This file
└── .gitignore                  # Git configuration
```

## Data Structure

Data is organized by location ID and date:
- **Location**: Restaurant location code (e.g., `90984`)
- **Date**: Folder in `YYYYMMDD` format (e.g., `20250116` for Jan 16, 2025)

Expected files in each date folder:
- CSV: `OrderDetails.csv`, `PaymentDetails.csv`, `TimeEntries.csv`, `ItemSelectionDetails.csv`, `CheckDetails.csv`
- Excel: `AccountingReport.xlsx`, `AllItemsReport.xlsx`, `CashEntries.xlsx`, `HouseAccountExport.xlsx`, `KitchenTimings.xlsx`, `ModifiersSelectionDetails.xlsx`
- JSON: Various formats (parsed per file)

## Features

- **Sales Analytics**: Revenue and order trends over time
- **Menu Performance**: Top items by revenue and order count
- **Inventory Tracking**: Stock levels and reorder alerts
- **Review Analysis**: Customer sentiment and category breakdowns
- **Data Upload**: Support for CSV file uploads with flexible column mapping
- **AI Assistant**: Query-based insights on dashboard data

## Development

See [docs/CLAUDE.md](./docs/CLAUDE.md) for detailed development guidance and architecture notes.

### Planned: Streamlit Conversion
- Replace React with Python/Streamlit
- Read data files from `data/` directory structure
- Streamlit Cloud deployment

## Git Repository

This project is a Git repository ready for GitHub deployment. See [docs/CLAUDE.md](./docs/CLAUDE.md) for repository guidelines.
