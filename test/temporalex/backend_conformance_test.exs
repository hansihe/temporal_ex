defmodule Temporalex.BackendConformanceTest do
  use ExUnit.Case, async: false

  alias Temporalex.Backend.Test, as: TestBackend
  alias Temporalex.Core.ActivityCompletion
  alias Temporalex.Core.ActivityTask
  alias Temporalex.Core.Activation
  alias Temporalex.Core.Command
  alias Temporalex.Core.Completion

  defmodule Workflow do
    use Temporalex.Workflow

    def run(input), do: {:ok, input}
  end

  setup do
    name = Module.concat(__MODULE__, :"Worker#{System.unique_integer([:positive])}")

    start_supervised!(
      {Temporalex.Worker,
       name: name, backend: TestBackend, test_owner: self(), workflows: [Workflow], activities: []}
    )

    %{worker: name}
  end

  test "test backend delivers workflow activations and captures workflow completions", %{
    worker: worker
  } do
    activation = %Activation{
      run_id: "run-backend-conformance",
      jobs: [
        %Temporalex.Core.Job.InitializeWorkflow{
          workflow_type: Workflow.__workflow_type__(),
          workflow_id: "wf",
          arguments: [:ok],
          workflow_info: %{},
          randomness_seed: 0
        }
      ]
    }

    assert :ok = TestBackend.send_activation(worker, activation)

    assert %Completion{
             run_id: "run-backend-conformance",
             status: {:ok, [%Command.CompleteWorkflow{result: :ok}]}
           } = TestBackend.fetch_workflow_completion(worker, "run-backend-conformance")

    assert_receive {:temporalex_test_backend, :workflow_completion, %Completion{}}
  end

  test "test backend delivers activity tasks and captures activity completions", %{worker: worker} do
    completion = %ActivityCompletion{
      task_token: "token",
      result: {:error, {:unknown_activity_type, "missing"}}
    }

    assert :ok =
             TestBackend.send_activity_task(worker, %ActivityTask{
               task_token: "token",
               activity_id: "activity",
               activity_type: "missing",
               input: [],
               variant: :start
             })

    assert ^completion = TestBackend.fetch_activity_completion(worker, "token")
    assert_receive {:temporalex_test_backend, :activity_completion, ^completion}
  end

  test "TemporalCore backend fails explicitly until native bridge exists" do
    assert {:error, {:not_implemented, message}} =
             Temporalex.Backend.TemporalCore.start_worker([], self())

    assert message =~ "Temporal Core/Rustler"
  end
end
