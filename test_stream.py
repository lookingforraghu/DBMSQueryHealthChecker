import requests
import json
import time

def test_stream():
    url = "http://127.0.0.1:5000/api/analyze/stream?workloadPath=c:/Users/raghu/OneDrive/Desktop/DBMS project/test_workload_enterprise.txt"
    start_time = time.time()
    
    with requests.get(url, stream=True) as r:
        for line in r.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if decoded_line.startswith('data:'):
                    try:
                        data = json.loads(decoded_line[5:])
                        if data.get('type') == 'progress':
                            print(f"[+] Progress: {data.get('done')} queries processed.")
                        elif data.get('type') == 'done':
                            print("[*] Stream ended successfully.")
                            break
                        elif data.get('type') == 'result':
                            print(f"[*] Received results! Total queries: {data.get('stats', {}).get('total_queries')}")
                    except json.JSONDecodeError:
                        print("Error parsing JSON:", decoded_line)
    
    end_time = time.time()
    print(f"[*] Total time taken: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    time.sleep(2) # Wait for server to be responsive
    test_stream()
