# Welcome to Chia

Here is our application and our implementation of Chia.

1) Start the venv
   python -m venv venv
.\venv\Scripts\Activate.ps1

2) Install dependencies
   pip install fastapi uvicorn requests pydantic

3) Run paypal:
cd C:\Users\<File Path>
.\venv\Scripts\Activate.ps1
uvicorn paypal.app:app --reload --port 8001

4) Run PencilPros
   cd C:\Users\<File Path>
.\venv\Scripts\Activate.ps1
uvicorn pencilpros.app:app --reload --port 8000

5) Open the Following:
PencilPros: http://127.0.0.1:8000/docs
PayPal: http://127.0.0.1:8001/docs

6) Add JSON commands to either

7) Run the inspect to view changes to db and prov log:
cd "C:\Users\<File Path>
.\venv\Scripts\Activate.ps1   # if not already active
python inspect_db.py
