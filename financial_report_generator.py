"""
Financial Report Generator
Fetches financial data from FMP API and generates Word documents
"""
import os
import sys
import tempfile
from datetime import datetime
from fmp_data_fetcher import fetch_fmp_financials, DataSourceError, FinancialSnapshot


def format_currency(value):
    """Format a number as currency with billions/millions notation."""
    if value is None:
        return "N/A"

    if abs(value) >= 1_000_000_000:
        return f"${value / 1_000_000_000:,.2f}B"
    elif abs(value) >= 1_000_000:
        return f"${value / 1_000_000:,.2f}M"
    else:
        return f"${value:,.0f}"


def generate_report_data(ticker, periods=5, period_type="annual"):
    """
    Fetch financial data and prepare it for reporting.

    Args:
        ticker: Stock ticker symbol
        periods: Number of periods to fetch
        period_type: "annual" or "quarter"

    Returns:
        Dictionary containing report data
    """
    print(f"Fetching {period_type} financial data for {ticker}...")

    try:
        snapshots = fetch_fmp_financials(ticker, limit=periods, period=period_type)

        report_data = {
            'ticker': ticker.upper(),
            'period_type': period_type.capitalize(),
            'generated_date': datetime.now().strftime("%B %d, %Y at %I:%M %p"),
            'num_periods': len(snapshots),
            'snapshots': snapshots
        }

        print(f"✓ Successfully fetched {len(snapshots)} periods of data")
        return report_data

    except DataSourceError as e:
        print(f"✗ Error fetching data: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        sys.exit(1)


def create_word_report(report_data, output_path):
    """Generate a Word document report using Node.js and docx library."""

    # Escape backslashes for Windows paths in JavaScript
    output_path_js = output_path.replace('\\', '\\\\')

    # Create JavaScript code to generate the Word document
    js_code = f"""
const {{ Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell, 
        AlignmentType, WidthType, HeadingLevel }} = require('docx');
const fs = require('fs');

// Report data
const ticker = "{report_data['ticker']}";
const periodType = "{report_data['period_type']}";
const generatedDate = "{report_data['generated_date']}";
const numPeriods = {report_data['num_periods']};

// Financial data
const snapshots = {str([{
    'period_label': snap.period_label,
    'revenue': snap.revenue,
    'cogs': snap.cogs,
    'operating_income': snap.operating_income,
    'net_income': snap.net_income,
    'operating_cash_flow': snap.operating_cash_flow,
    'capex': snap.capex,
    'total_debt': snap.total_debt,
    'cash': snap.cash,
    'accounts_receivable': snap.accounts_receivable,
    'inventory': snap.inventory,
    'goodwill': snap.goodwill,
    'share_based_comp': snap.share_based_comp,
    'shares_outstanding': snap.shares_outstanding
} for snap in report_data['snapshots']]).replace('None', 'null')};

// Helper function to format currency
function formatCurrency(value) {{
    if (value === null || value === undefined) return "N/A";
    if (Math.abs(value) >= 1000000000) {{
        return "$" + (value / 1000000000).toFixed(2) + "B";
    }} else if (Math.abs(value) >= 1000000) {{
        return "$" + (value / 1000000).toFixed(2) + "M";
    }}
    return "$" + value.toLocaleString('en-US', {{ maximumFractionDigits: 0 }});
}}

// Create document
const doc = new Document({{
    styles: {{
        default: {{
            document: {{ run: {{ font: "Arial", size: 22 }} }}
        }},
        paragraphStyles: [
            {{
                id: "Title",
                name: "Title",
                basedOn: "Normal",
                run: {{ size: 56, bold: true, color: "1F4E78", font: "Arial" }},
                paragraph: {{ spacing: {{ before: 240, after: 120 }}, alignment: AlignmentType.CENTER }}
            }},
            {{
                id: "Heading1",
                name: "Heading 1",
                basedOn: "Normal",
                next: "Normal",
                quickFormat: true,
                run: {{ size: 32, bold: true, color: "1F4E78", font: "Arial" }},
                paragraph: {{ spacing: {{ before: 240, after: 120 }}, outlineLevel: 0 }}
            }},
            {{
                id: "Heading2",
                name: "Heading 2",
                basedOn: "Normal",
                next: "Normal",
                quickFormat: true,
                run: {{ size: 28, bold: true, color: "2E5C8A", font: "Arial" }},
                paragraph: {{ spacing: {{ before: 180, after: 100 }}, outlineLevel: 1 }}
            }}
        ]
    }},
    sections: [{{
        properties: {{
            page: {{ margin: {{ top: 1440, right: 1440, bottom: 1440, left: 1440 }} }}
        }},
        children: [
            // Title
            new Paragraph({{
                heading: HeadingLevel.TITLE,
                children: [new TextRun(`Financial Analysis Report`)]
            }}),

            // Subtitle
            new Paragraph({{
                alignment: AlignmentType.CENTER,
                spacing: {{ after: 400 }},
                children: [
                    new TextRun({{
                        text: `${{ticker}} - ${{periodType}} Statements`,
                        size: 28,
                        color: "666666"
                    }})
                ]
            }}),

            // Report info
            new Paragraph({{
                spacing: {{ after: 200 }},
                children: [
                    new TextRun({{
                        text: `Generated: ${{generatedDate}}`,
                        size: 20,
                        color: "666666"
                    }})
                ]
            }}),

            new Paragraph({{
                spacing: {{ after: 400 }},
                children: [
                    new TextRun({{
                        text: `Periods Analyzed: ${{numPeriods}}`,
                        size: 20,
                        color: "666666"
                    }})
                ]
            }}),

            // Income Statement section
            new Paragraph({{
                heading: HeadingLevel.HEADING_1,
                spacing: {{ before: 400 }},
                children: [new TextRun("Income Statement")]
            }}),

            // Income Statement Table
            new Table({{
                width: {{ size: 100, type: WidthType.PERCENTAGE }},
                rows: [
                    // Header row
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{
                                    children: [new TextRun({{ text: "Metric", bold: true, color: "FFFFFF" }})]
                                }})],
                                shading: {{ fill: "1F4E78" }}
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.CENTER,
                                        children: [new TextRun({{ text: snap.period_label, bold: true, color: "FFFFFF" }})]
                                    }})],
                                    shading: {{ fill: "1F4E78" }}
                                }})
                            )
                        ]
                    }}),
                    // Revenue row
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Revenue")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.revenue))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    // COGS row
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Cost of Revenue")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.cogs))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    // Operating Income row
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Operating Income")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.operating_income))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    // Net Income row
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun({{ text: "Net Income", bold: true }})] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun({{ text: formatCurrency(snap.net_income), bold: true }})]
                                    }})],
                                    shading: {{ fill: "F2F2F2" }}
                                }})
                            )
                        ]
                    }})
                ]
            }}),

            // Cash Flow section
            new Paragraph({{
                heading: HeadingLevel.HEADING_1,
                spacing: {{ before: 400 }},
                children: [new TextRun("Cash Flow Statement")]
            }}),

            new Table({{
                width: {{ size: 100, type: WidthType.PERCENTAGE }},
                rows: [
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun({{ text: "Metric", bold: true, color: "FFFFFF" }})] }})],
                                shading: {{ fill: "2E5C8A" }}
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.CENTER,
                                        children: [new TextRun({{ text: snap.period_label, bold: true, color: "FFFFFF" }})]
                                    }})],
                                    shading: {{ fill: "2E5C8A" }}
                                }})
                            )
                        ]
                    }}),
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Operating Cash Flow")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.operating_cash_flow))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Capital Expenditure")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.capex))]
                                    }})]
                                }})
                            )
                        ]
                    }})
                ]
            }}),

            // Balance Sheet section
            new Paragraph({{
                heading: HeadingLevel.HEADING_1,
                spacing: {{ before: 400 }},
                children: [new TextRun("Balance Sheet")]
            }}),

            new Table({{
                width: {{ size: 100, type: WidthType.PERCENTAGE }},
                rows: [
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun({{ text: "Metric", bold: true, color: "FFFFFF" }})] }})],
                                shading: {{ fill: "406080" }}
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.CENTER,
                                        children: [new TextRun({{ text: snap.period_label, bold: true, color: "FFFFFF" }})]
                                    }})],
                                    shading: {{ fill: "406080" }}
                                }})
                            )
                        ]
                    }}),
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Cash")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.cash))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Total Debt")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.total_debt))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Accounts Receivable")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.accounts_receivable))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Inventory")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.inventory))]
                                    }})]
                                }})
                            )
                        ]
                    }}),
                    new TableRow({{
                        children: [
                            new TableCell({{
                                children: [new Paragraph({{ children: [new TextRun("Goodwill")] }})]
                            }}),
                            ...snapshots.map(snap => 
                                new TableCell({{
                                    children: [new Paragraph({{
                                        alignment: AlignmentType.RIGHT,
                                        children: [new TextRun(formatCurrency(snap.goodwill))]
                                    }})]
                                }})
                            )
                        ]
                    }})
                ]
            }})
        ]
    }}]
}});

// Generate and save
Packer.toBuffer(doc).then(buffer => {{
    fs.writeFileSync("{output_path_js}", buffer);
    console.log("✓ Word document created successfully!");
}}).catch(err => {{
    console.error("✗ Error creating document:", err);
    process.exit(1);
}});
"""

    # Write JavaScript to file in project directory
    js_file = 'temp_report.js'
    with open(js_file, 'w', encoding='utf-8') as f:
        f.write(js_code)

    try:
        # Execute JavaScript
        print(f"Generating Word document: {output_path}")
        import subprocess
        result = subprocess.run(["node", js_file], capture_output=True, text=True)

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("Error details:")
            print(result.stderr)

        if result.returncode == 0:
            print(f"\nSuccess! Word document created at:")
            print(f"  {output_path}")
        else:
            print(f"\nError: Node.js exited with code {result.returncode}")
    finally:
        # Clean up temp file
        try:
            os.remove(js_file)
        except:
            pass


def main():
    """Main function to generate financial reports."""

    # Configuration
    ticker = input("Enter stock ticker (e.g., AAPL): ").strip().upper()
    if not ticker:
        ticker = "AAPL"  # Default

    period_type = input("Enter period type (annual/quarter) [annual]: ").strip().lower()
    if period_type not in ["annual", "quarter"]:
        period_type = "annual"

    try:
        periods = int(input("Number of periods to analyze [5]: ").strip() or "5")
    except ValueError:
        periods = 5

    # Generate report data
    report_data = generate_report_data(ticker, periods, period_type)

    # Create output filename in current directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    word_output = f"{ticker}_Financial_Report_{timestamp}.docx"

    # Generate Word document
    create_word_report(report_data, word_output)

    print(f"\n{'=' * 60}")
    print(f"Report generation complete!")
    print(f"File location: {os.path.abspath(word_output)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()