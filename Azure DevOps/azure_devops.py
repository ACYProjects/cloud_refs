from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
from azure.devops.v6_0.core.models import TeamProject, ProjectVisibility
from azure.devops.v6_0.work_item_tracking.models import JsonPatchOperation
from azure.devops.v6_0.git.models import GitRepositoryCreateOptions
from azure.devops.v6_0.build.models import BuildDefinition, BuildRepository

personal_access_token = ''
organization_url = 'https://dev.azure.com/your_organization'

credentials = BasicAuthentication('', personal_access_token)
connection = Connection(base_url=organization_url, creds=credentials)

core_client = connection.clients.get_core_client()
work_item_client = connection.clients.get_work_item_tracking_client()
git_client = connection.clients.get_git_client()
build_client = connection.clients.get_build_client()

def create_project(project_name):
    print(f"Creating project: {project_name}")
    project = TeamProject(name=project_name, visibility=ProjectVisibility.private, capabilities={'versioncontrol': {'sourceControlType': 'Git'}, 'processTemplate': {'templateTypeId': '6b724908-ef14-45cf-84f8-768b5384da45'}})
    operation_reference = core_client.queue_create_project(project)
    return operation_reference

def create_work_item(project_name, title):
    print(f"Creating work item: {title}")
    ops = [
        JsonPatchOperation(
            op="add",
            path="/fields/System.Title",
            value=title
        )
    ]
    work_item = work_item_client.create_work_item(document=ops, project=project_name, type="Task")
    return work_item

def create_repository(project_name, repo_name):
    print(f"Creating repository: {repo_name}")
    repo_options = GitRepositoryCreateOptions(name=repo_name)
    repository = git_client.create_repository(git_repository_to_create=repo_options, project=project_name)
    return repository

def create_build_pipeline(project_name, repo_id):
    print("Creating build pipeline")
    build_definition = BuildDefinition(
        name="Sample Build Pipeline",
        repository=BuildRepository(
            id=repo_id,
            type="TfsGit"
        ),
        process={
            "type": 2,  # YAML
            "yamlFilename": "/azure-pipelines.yml"
        }
    )
    created_definition = build_client.create_definition(definition=build_definition, project=project_name)
    return created_definition

if __name__ == "__main__":
    project_name = "SampleProject"
    
    project_reference = create_project(project_name)
    print(f"Project creation initiated. Operation ID: {project_reference.id}")
    
    import time
    time.sleep(30)
    
    work_item = create_work_item(project_name, "Sample Task")
    print(f"Work item created. ID: {work_item.id}")
    
    repository = create_repository(project_name, "SampleRepo")
    print(f"Repository created. ID: {repository.id}")
    
    pipeline = create_build_pipeline(project_name, repository.id)
    print(f"Build pipeline created. ID: {pipeline.id}")
