#!/usr/bin/env python3
"""
Analyzes dbt run_results.json using OpenAI o3-mini and sends email summary
"""

import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from openai import OpenAI
import sys

def load_run_results():
    """Load the dbt run results from target directory"""
    results_path = "target/run_results.json"
    if not os.path.exists(results_path):
        print(f"Error: {results_path} not found")
        sys.exit(1)
        
    with open(results_path, 'r') as f:
        return json.load(f)

def analyze_with_openai(run_results):
    """Analyze run results using OpenAI o3-mini"""
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    
    # Prepare the data for analysis
    summary_data = {
        'metadata': run_results.get('metadata', {}),
        'results': []
    }
    
    # Extract key information from results
    for result in run_results.get('results', []):
        summary_data['results'].append({
            'unique_id': result.get('unique_id'),
            'status': result.get('status'),
            'execution_time': result.get('execution_time'),
            'message': result.get('message'),
            'failures': result.get('failures', 0),
            'adapter_response': result.get('adapter_response', {})
        })
    
    prompt = f"""
    Analyze this dbt run results and provide a concise summary focusing on:
    1. Overall run status and success rate
    2. Any failures or errors with specific model names
    3. Performance insights (slow-running models)
    4. Recommendations for issues found
    
    dbt Run Results:
    {json.dumps(summary_data, indent=2)}
    
    Please provide a clear, actionable summary in HTML format suitable for email.
    """
    
    try:
        response = client.chat.completions.create(
            model="o3-mini",
            messages=[
                {"role": "system", "content": "You are a data engineering expert analyzing dbt run results. Provide clear, actionable summaries."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1500,
            temperature=0.3
        )
        
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return create_fallback_summary(run_results)

def create_fallback_summary(run_results):
    """Create a basic summary if OpenAI API fails"""
    total_models = len(run_results.get('results', []))
    successful = len([r for r in run_results.get('results', []) if r.get('status') == 'success'])
    failed = len([r for r in run_results.get('results', []) if r.get('status') == 'error'])
    
    failed_models = [r.get('unique_id') for r in run_results.get('results', []) if r.get('status') == 'error']
    
    html_content = f"""
    <h2>dbt Run Summary (Fallback)</h2>
    <p><strong>Total Models:</strong> {total_models}</p>
    <p><strong>Successful:</strong> {successful}</p>
    <p><strong>Failed:</strong> {failed}</p>
    
    {f"<p><strong>Failed Models:</strong></p><ul>{''.join([f'<li>{model}</li>' for model in failed_models])}</ul>" if failed_models else ""}
    
    <p><em>Note: This is a basic summary due to OpenAI API unavailability.</em></p>
    """
    
    return html_content

def send_email(subject, html_content):
    """Send email with the analysis results"""
    email_host = os.getenv('EMAIL_HOST')
    email_port = int(os.getenv('EMAIL_PORT', 587))
    email_user = os.getenv('EMAIL_USER')
    email_password = os.getenv('EMAIL_PASSWORD')
    recipient_email = os.getenv('RECIPIENT_EMAIL')
    
    if not all([email_host, email_user, email_password, recipient_email]):
        print("Error: Email configuration incomplete")
        sys.exit(1)
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = email_user
    msg['To'] = recipient_email
    
    # Create HTML part
    html_part = MIMEText(html_content, 'html')
    msg.attach(html_part)
    
    try:
        server = smtplib.SMTP(email_host, email_port)
        server.starttls()
        server.login(email_user, email_password)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")
        sys.exit(1)

def main():
    """Main function"""
    print("Starting dbt run results analysis...")
    
    # Load run results
    run_results = load_run_results()
    
    # Analyze with OpenAI
    analysis = analyze_with_openai(run_results)
    
    # Determine overall status for subject
    total_models = len(run_results.get('results', []))
    failed = len([r for r in run_results.get('results', []) if r.get('status') == 'error'])
    
    if failed > 0:
        status = f"⚠️ FAILURES DETECTED ({failed}/{total_models} failed)"
    else:
        status = f"✅ SUCCESS ({total_models}/{total_models} passed)"
    
    subject = f"Weekly dbt Run Report - {status} - {datetime.now().strftime('%Y-%m-%d')}"
    
    # Send email
    send_email(subject, analysis)
    
    print("Analysis complete!")

if __name__ == "__main__":
    main()