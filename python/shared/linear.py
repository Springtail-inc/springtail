import requests
import json
import os

class Linear:
    """Linear API class.

    Example Usage:
        linear = Linear()

        team_id = linear.get_team_id('Springtail')
        label_id = linear.get_label_id('Test Error Report', 'Springtail')

        linear.set_team('Springtail')

        asset_url = linear.upload_file('/tmp/gc.log.gz')
        issue = linear.create_linear_issue_with_label('Test error report description', 'Test Error with Asset', label_id, asset_url)
        issue = linear.create_bug_report_with_file('Test Error with Asset', 'Test error report description', '/tmp/gc.log.gz')

        print(f"Created issue: {issue['url']}")
     """

    def __init__(self):
        """Initialize the Linear API."""
        path = os.path.expanduser('~/.linear_env')

        self.DEFAULT_TEAM = 'Springtail'
        self.LABEL_ERROR_REPORT = 'Test Error Report'

        if os.path.exists(path):
            # read file as json
            with open(path, 'r') as f:
                data = json.load(f)

            self.api_key = data['api_key']
            self.team_id = data['team_id']
        else:
            self.api_key = os.environ.get('LINEAR_API_KEY')
            self.team_id = os.environ.get('LINEAR_TEAM_ID')

        if not self.api_key:
            raise Exception("Linear API key and team ID not found in environment variables or .linear_env file")

        self.url = "https://api.linear.app/graphql"
        self.labels = {}
        self.teams = {}


    def get_labels(self, team_name='Springtail'):
        """Get the labels for the team."""
        team_id = self.get_team_id(team_name)

        query = f"""
        {{
            team(id: "{team_id}") {{
                labels {{
                    nodes {{
                        id
                        name
                    }}
                }}
            }}
        }}
        """

        r = self.issue_query(query, {})
        labels = r['team']['labels']['nodes']

        for label in labels:
            self.labels[label['name']] = label['id']

        return labels


    def create_linear_issue_with_label(self, description, title, label_id, asset_url=None):
        """Create a Linear issue with the given tag and label."""
        query = """
        mutation ($input: IssueCreateInput!) {
        issueCreate(input: $input) {
            success
                issue {
                    id
                    url
                }
            }
        }
        """

        variables = {
            "input": {
                "teamId": self.team_id,  # Replace with your team ID
                "title": title,
                "description": description,
                "labelIds": [label_id]  # Use the provided label/tag ID
            }
        }

        if asset_url:
            variables['input']['description'] = description + f"\nAttached logs: [Click to download]({asset_url})"

        print("Variables: ", variables['input'])

        r = self.issue_query(query, variables)
        print(f"Create issue response: {r}")

        if r['issueCreate']['success']:
            return r['issueCreate']['issue']

        raise Exception(f"Error creating issue: {r}")


    def upload_file(self, file_path):
        """Upload a file to Linear."""
        # Get file details
        filename = os.path.basename(file_path)
        size = os.path.getsize(file_path)
        content_type = "application/octet-stream"

        # GraphQL mutation
        mutation = """
        mutation FileUpload($size: Int!, $contentType: String!, $filename: String!) {
            fileUpload(size: $size, contentType: $contentType, filename: $filename) {
                uploadFile { uploadUrl assetUrl headers { key value } }
            }
        }
        """

        # GraphQL variables
        variables = {
            "size": size,
            "contentType": content_type,
            "filename": filename
        }

        # Issue the query
        r = self.issue_query(mutation, variables)

        # Get the upload URL
        data = r['fileUpload']['uploadFile']
        upload_url = data['uploadUrl']
        asset_url = data['assetUrl']

        # Set the headers
        headers = {
            "Content-Type": content_type,
            "Cache-Control": "public, max-age=31536000"
        }
        # Add the headers from the response
        for kv in data['headers']:
            headers[kv['key']] = kv['value']

        # Upload the file
        with open(file_path, 'rb') as f:
            r = requests.put(upload_url, data=f.read(), headers=headers)
            if r.status_code != 200:
                raise Exception(f"Error uploading file: {r.text}")

        return asset_url


    def resource_query(self, resource):
        """Query the Linear API for the given resource."""
        query = f"""
        {{
            {resource} {{
                nodes {{
                    id
                    name
                }}
            }}
        }}
        """

        r = self.issue_query(query, {})

        return r[resource]['nodes']


    def get_teams(self):
        """Get the teams for the user."""
        if not self.teams:
            teams = self.resource_query('teams')
            for team in teams:
                self.teams[team['name']] = team['id']

        return self.teams


    def get_team_id(self, team_name):
        """Get the team ID for the given team name."""
        if not self.teams:
            self.get_teams()

        if team_name not in self.teams:
            raise Exception(f"Team not found: {team_name}")

        return self.teams[team_name]


    def get_label_id(self, label_name, team_name=None):
        """Get the label ID for the given label name."""
        if not self.labels:
            self.get_labels(team_name)

        if label_name not in self.labels:
            raise Exception(f"Label not found: {label_name}")

        return self.labels[label_name]


    def set_team(self, team_name):
        """Set the team ID."""
        self.team_id = self.get_team_id(team_name)


    def create_bug_report_with_file(self, title, description, file_path):
        """Create a bug report with the given title, description, and file."""
        asset_url = None
        try:
            asset_url = self.upload_file(file_path)
        except:
            pass

        label_id = self.get_label_id(self.LABEL_ERROR_REPORT, self.DEFAULT_TEAM)
        return self.create_linear_issue_with_label(description, title, label_id, asset_url)


    def issue_query(self, query, variables={}):
        """Query the Linear API."""
        headers = {
            "Authorization": f"{self.api_key}",
            "Content-Type": "application/json"
        }

        response = requests.post(self.url, json={"query": query, "variables": variables}, headers=headers)

        if response.status_code != 200:
            if 'errors' in response.json():
                print(f"Error in query: {response.json()['errors']}")
                raise Exception(f"Error in query: error={response.json()['errors']}")
            print(f"Error in query: {response.text}")
            raise Exception(f"Error in query: status={response.status_code}, error={response.text}")

        json_response = response.json()
        if 'errors' in json_response:
            print(f"Error in query: {json_response['errors']}")
            raise Exception(f"Error in query: error={json_response['errors']}")

        if 'data' in json_response:
            json_response = json_response['data']

            # get the headers into the resonse
            #print(f"Response headers: {response.headers}")
            #json_response['headers'] = response.headers

            return json_response

        return json_response

# Example Usage:
#
# linear = Linear()
#
# team_id = linear.get_team_id('Springtail')
# label_id = linear.get_label_id('Test Error Report', 'Springtail')
#
# linear.set_team('Springtail')
#
# asset_url = linear.upload_file('/tmp/gc.log.gz')
#
# issue = linear.create_linear_issue_with_label('Test error report description\nTest more', 'Test Error with Asset', label_id, asset_url)
# print(f"Created issue: {issue['url']}")

