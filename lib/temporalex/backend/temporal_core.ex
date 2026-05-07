defmodule Temporalex.Backend.TemporalCore do
  @moduledoc """
  Placeholder for the real Temporal Core backend.

  The server/backend boundary is implemented and tested through
  `Temporalex.Backend.Test`. This module exists so alpha users get an explicit
  failure instead of accidentally assuming the native Temporal Core/Rustler
  bridge is available.
  """

  @behaviour Temporalex.Backend

  @impl Temporalex.Backend
  def start_worker(_opts, _owner_pid) do
    {:error,
     {:not_implemented,
      "Temporalex.Backend.TemporalCore requires the native Temporal Core/Rustler bridge, which is not implemented yet"}}
  end

  @impl Temporalex.Backend
  def complete_workflow_activation(_state, _completion), do: {:error, :not_started}

  @impl Temporalex.Backend
  def complete_activity_task(_state, _completion), do: {:error, :not_started}

  @impl Temporalex.Backend
  def shutdown_worker(_state), do: :ok
end
