defmodule Temporalex.Activity.Context do
  @moduledoc """
  Runtime context passed to activity implementations that declare a context
  argument.

  Heartbeats are local-only for the alpha server boundary. The real backend can
  extend this module to submit heartbeat payloads through Temporal Core.
  """

  defstruct activity_id: nil,
            activity_type: nil,
            task_token: nil,
            workflow_id: nil,
            workflow_type: nil,
            workflow_namespace: nil,
            run_id: nil,
            task_queue: nil,
            attempt: 1,
            heartbeat_timeout: nil,
            is_local: false,
            worker: nil,
            cancelled: nil,
            cancel_reason: nil

  def heartbeat(%__MODULE__{} = context, _details \\ nil) do
    if cancelled?(context) do
      {:cancelled, context.cancel_reason || :cancelled}
    else
      :ok
    end
  end

  def cancelled?(%__MODULE__{cancelled: nil}), do: false

  def cancelled?(%__MODULE__{cancelled: cancelled}) do
    :atomics.get(cancelled, 1) == 1
  end
end
