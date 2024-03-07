import requests

def get_latest_issue_pr_numbers(owner, repo):
    # Define the GitHub API URL for issues and pull requests
    issues_url = f"https://api.github.com/repos/{owner}/{repo}/issues"
    prs_url = f"https://api.github.com/repos/{owner}/{repo}/pulls"

    # Fetch the most recent issue and pull request
    latest_issue_response = requests.get(issues_url, params={'state': 'all', 'per_page': 1})
    latest_pr_response = requests.get(prs_url, params={'state': 'all', 'per_page': 1})

    # Check for successful responses
    if latest_issue_response.status_code == 200 and latest_pr_response.status_code == 200:
        latest_issue = latest_issue_response.json()
        latest_pr = latest_pr_response.json()

        # Extract issue and pull request numbers from URLs
        issue_number = int(latest_issue[0]['number']) if latest_issue else 0
        pr_number = int(latest_pr[0]['number']) if latest_pr else 0

        return max(issue_number, pr_number)
    else:
        print("Failed to fetch data from GitHub API")

# Example usage
lm = get_latest_issue_pr_numbers('rqlite', 'rqlite')

print(lm+1)

