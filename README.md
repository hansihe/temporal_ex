# Temporalex

Temporalex is an experimental Elixir SDK for Temporal. The current alpha surface includes:

- deterministic workflow execution in pure Elixir
- activities, durable timers, signals, updates, queries, `parallel`, and `phase`
- a worker/server supervision tree
- an in-memory backend for integration tests and SDK evaluation

The native Temporal Core/Rustler backend is not implemented yet. `Temporalex.Backend.TemporalCore` fails explicitly until that bridge exists.

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

## Verification

Run the default suite:

```bash
mix test
```

If the Temporal CLI is installed, run the external smoke check:

```bash
mix test --only external
```

That check starts a headless `temporal server start-dev`, waits for a CLI health check, and then shuts it down. Full Temporal workflow execution requires the future native backend.

The completed core review gates are recorded in `docs/review_gates.md`.
