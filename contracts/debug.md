curl -X POST http://localhost:8080/api/v2/dags/contract_provisioning/dagRuns -H "Content-Type: application/json" -u admin:U4AqRFg3VDFyquze -d '{"conf": {"github_pr_number": 42}}'


  curl -X GET http://localhost:8080/api/v2/dags -u admin:U4AqRFg3VDFyquze                                                                                                                                                                                                                                                               



  This will:                                                                                                                                                                                             
  1. Create a public HTTPS tunnel to your local http://localhost:8080                                                                                                                                    
  2. Give you a URL like https://xxx-xxx-xxx-xxx.ngrok.io                                                                                                                                                
  3. Use that URL as your AIRFLOW_URL in GitHub Actions secrets                                                                                                                                          
                                                                                                                                                                                                         
  Setup steps:                                                                                                                                                                                           
                                                                                                                                                                                                         
  1. Install ngrok (if not already installed):                                                                                                                                                           
  brew install ngrok  # macOS                                                                                                                                                                            
  # or download from https://ngrok.com/download                                                                                                                                                          
  2. Start ngrok tunnel:                                                                                                                                                                                 
  ngrok http 8080                                                                                                                                                                                        
  3. Copy the forwarding URL (e.g., https://xxx-xxx-xxx-xxx.ngrok.io)                                                                                                                                    
  4. Update GitHub secrets with:                                                                                                                                                                         
    - AIRFLOW_URL: https://xxx-xxx-xxx-xxx.ngrok.io                                                                                                                                                      
    - AIRFLOW_USERNAME: admin                                                                                                                                                                            
    - AIRFLOW_PASSWORD: admin                         




  1. Trigger the DAG:                                                                                                                                                                                    
  curl -s -w "\n%{http_code}" -X POST \                                                                                                                                                                  
    "http://localhost:8080/api/v1/dags/contract_provisioning/dagRuns" \                                                                                                                                  
    -H "Content-Type: application/json" \                                                                                                                                                                
    -u "admin:admin" \                                                                                                                                                                                   
    -d '{                                                                                                                                                                                                
      "conf": {                                                                                                                                                                                          
        "github_pr_number": 3,                                                                                                                                                                           
        "github_repo": "vanikkars/data-contract-flow",                                                                                                                                                   
        "github_token": "your-github-token"                                                                                                                                                              
      }                                                                                                                                                                                                  
    }'                                                                                                                                                                                                   
                                                                                                                                                                                                         
  2. Poll DAG status (repeatedly until completion):
```aiignore
  curl -s -X GET "http://localhost:8080/api/v1/dags/contract_provisioning/dagRuns/manual__2026-08-12T10:14:35.651938+00:00" -u "admin:admin"                                                                                                          
```
                                                                           
  This returns JSON with a state field: success, failed, running, etc.                                                                                                                                   
                                                                                                                                                                                                         
  3. Fetch XCom results (after DAG completes):                                                                                                                                                           
  curl -s -X GET \                                                                                                                                                                                       
    "http://localhost:8080/api/v1/dags/contract_provisioning/dagRuns/manual__2026-08-12T10:14:35.651938+00:00/taskInstances/collect_and_format_results/xcomEntries?key=github_payload" \                 
    -u "admin:admin"                                                                                                                                                                                     
  This returns the GitHub comment payload that was pushed by the DAG.                                                                                                                                    
                                                                                                                                                                                                         
  To test manually, replace:                                                                                                                                                                             
  - http://localhost:8080 with your ngrok URL (e.g., https://xxx-xxx-xxx-xxx.ngrok.io)                                                                                                                   
  - manual__2026-08-12T10:14:35.651938+00:00 with an actual DAG run ID from your Airflow instance                                                                                                        
                                                                                                                                                                                                         
  You can get a real DAG run ID with:                                                                                                                                                                    
  curl -s -X GET \                                                                                                                                                                                       
    "http://localhost:8080/api/v1/dags/contract_provisioning/dagRuns?limit=1" \                                                                                                                          
    -u "admin:admin" | python3 -c "import sys, json; d = json.load(sys.stdin); print(d['dag_runs'][0]['dag_run_id'])"