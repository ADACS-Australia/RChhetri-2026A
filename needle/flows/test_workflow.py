from prefect import flow


@flow(log_prints=True)
def hello_world_flow(name: str = "world"):
    print(f"Hello {name}!")
