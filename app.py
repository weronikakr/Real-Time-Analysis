from flask import Flask, request, jsonify
import time
import pandas as pd

app = Flask(__name__)

@app.route('/', methods=['GET'])
def receive_data():
    aktualny_czas_s = time.time()
    liczba_wierszy = df.shape[0]
    liczba_porz = int(aktualny_czas_s) % liczba_wierszy
    
    
    data_row = df.iloc[liczba_porz]
    
    
    if not data_row.empty:
        print(data_row)
        return jsonify({'status': 'success', 'received': data_row.to_dict()}), 200
    else:
        return jsonify({'status': 'error', 'message': 'No data received'}), 400

if __name__ == '__main__':
    df = pd.read_csv('heartrate_seconds_merged.csv')
    app.run(debug=True, port=5000)
