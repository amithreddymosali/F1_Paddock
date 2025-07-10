# F1 Paddock Race Data Pipeline

This project is a data pipeline for processing and analyzing Formula 1 race data. It collects, transforms, and stores race data from various sources to enable in-depth race analytics and insights.
Features include collecting race data from the Jolpico API and FastF1 library, processing and normalizing large datasets, storing data in a PostgreSQL database, and supporting querying of race statistics and driver performance. The codebase is modular and uses Python virtual environments.

Prerequisites are Python 3.8 or higher, PostgreSQL database, and Git.

To get started, clone the repo with:
  git clone https://github.com/amithreddymosali/F1_Paddock.git
  cd F1_Paddock

Create and activate a virtual environment:
  python3 -m venv venv
  source venv/bin/activate   # (Windows: venv\Scripts\activate)

Install dependencies:
  pip install -r requirements.txt

Configure your database connection in the config file, then run the data pipeline scripts.

Contributions are welcome! Please open issues or submit pull requests.

This project is licensed under the MIT License. See the LICENSE file for details.

- Amith Reddy Mosali
