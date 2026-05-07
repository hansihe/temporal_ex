# Temporalex

Temporalex is an experimental Elixir SDK for Temporal. The current beta-evaluation surface includes:

- deterministic workflow execution in pure Elixir
- activities, durable timers, signals, updates, queries, `parallel`, and `phase`
- a worker/server supervision tree
- an in-memory backend for integration tests and SDK evaluation
- a native Temporal Core/Rustler backend that runs against Temporal Server
- a Temporal Core-backed client API for start, result, signal, query, update, cancel, terminate, and describe operations

## Quick Evaluation

Define a workflow and activity:

```elixir
defmodule MyApp.Activities do
  use Temporalex.Activity

  defactivity echo(value) do
    {:ok, value}
  end
end

defmodule MyApp.Workflow do
  use Temporalex.Workflow

  def run(value) do
    {:ok, result} = MyApp.Activities.echo(value)
    {:ok, result}
  end
end
```

Start a worker with the test backend:

```elixir
start_supervised!(
  {Temporalex.Worker,
   name: MyApp.Temporal,
   backend: Temporalex.Backend.Test,
   workflows: [MyApp.Workflow],
   activities: [MyApp.Activities]}
)
```

Tests can drive the server with core structs through `Temporalex.Backend.Test`. See `test/temporalex/server_integration_test.exs` for full activation and activity-task transcripts.

Start a worker against Temporal Server:

```elixir
{:ok, _worker} =
  Temporalex.Worker.start_link(
    name: MyApp.Temporal,
    backend: Temporalex.Backend.TemporalCore,
    target: "http://127.0.0.1:7233",
    namespace: "default",
    task_queue: "my-task-queue",
    workflows: [MyApp.Workflow],
    activities: [MyApp.Activities]
  )

{:ok, handle} =
  Temporalex.Client.start_workflow(MyApp.Temporal, MyApp.Workflow, %{order_id: 123},
    workflow_id: "order-123"
  )

{:ok, result} = Temporalex.Client.get_result(handle)
```

Long-lived workflows can also be driven through the handle:

```elixir
:ok = Temporalex.Client.signal_workflow(handle, "approve", [%{approved_by: "alice"}])
{:ok, state} = Temporalex.Client.query_workflow(handle, "status")
{:ok, reply} = Temporalex.Client.update_workflow(handle, "add_item", [%{sku: "ABC"}])
{:ok, description} = Temporalex.Client.describe_workflow(handle)
```

## Verification

Run the default suite:

```bash
CARGO_HOME=$(pwd)/.cargo-home mix test
```

If the Temporal CLI is installed, run the external smoke check:

```bash
CARGO_HOME=$(pwd)/.cargo-home mix test --only external
```

That check starts a headless `temporal server start-dev`, waits for a CLI health check, runs end-to-end workflows through the Rust NIF and Temporal Core, exercises client signal/query/update/describe/terminate paths, and then shuts it down.

The completed core and native review gates are recorded in `docs/review_gates.md`.
