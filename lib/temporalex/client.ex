defmodule Temporalex.Client do
  @moduledoc """
  Minimal client API for starting workflows through a running `Temporalex.Worker`.

  The first implementation is intentionally backed by the worker's Temporal Core
  connection. This keeps native resources inside `Temporalex.Backend.TemporalCore`
  while exposing only Elixir terms and workflow handles here.
  """

  alias Temporalex.Backend.TemporalCore

  defmodule Handle do
    @moduledoc """
    Handle for a started workflow execution.
    """

    defstruct [:worker, :workflow_id, :run_id, :workflow_type]
  end

  def start_workflow(worker, workflow, input, opts \\ []) when is_list(opts) do
    with {:ok, server} <- server_pid(worker),
         {:ok, state} <- temporal_core_state(server),
         workflow_type <- workflow_type(workflow),
         {:ok, info} <- TemporalCore.start_workflow(state, workflow_type, input, opts) do
      {:ok,
       %Handle{
         worker: worker,
         workflow_id: Map.fetch!(info, :workflow_id),
         run_id: Map.get(info, :run_id),
         workflow_type: Map.get(info, :workflow_type, workflow_type)
       }}
    end
  end

  def get_result(%Handle{} = handle, opts \\ []) when is_list(opts) do
    with {:ok, server} <- server_pid(handle.worker),
         {:ok, state} <- temporal_core_state(server) do
      TemporalCore.get_workflow_result(state, handle.workflow_id, handle.run_id, opts)
    end
  end

  defp server_pid(worker) do
    case Temporalex.Worker.server_pid(worker) do
      nil -> {:error, {:worker_not_started, worker}}
      pid when is_pid(pid) -> {:ok, pid}
    end
  end

  defp temporal_core_state(server) do
    case Temporalex.Server.backend_state(server) do
      %TemporalCore.State{} = state -> {:ok, state}
      other -> {:error, {:unsupported_backend, other}}
    end
  end

  defp workflow_type(workflow_type) when is_binary(workflow_type), do: workflow_type

  defp workflow_type(workflow_module) when is_atom(workflow_module) do
    if function_exported?(workflow_module, :__workflow_type__, 0) do
      workflow_module.__workflow_type__()
    else
      inspect(workflow_module)
    end
  end
end
