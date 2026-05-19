defmodule Temporalex.Backend.TemporalCore.Codec do
  @moduledoc false

  alias Temporalex.Backend.TemporalCore.PayloadConverter
  alias Temporalex.Backend.TemporalCore.Proto.Schema
  alias Temporalex.Core.ActivityCompletion
  alias Temporalex.Core.Command
  alias Temporalex.Core.Completion
  alias Temporalex.Failure

  @activity_task_completion :"coresdk.ActivityTaskCompletion"
  @activity_heartbeat :"coresdk.ActivityHeartbeat"
  @activity_task :"coresdk.activity_task.ActivityTask"
  @workflow_activation :"coresdk.workflow_activation.WorkflowActivation"
  @workflow_activation_completion :"coresdk.workflow_completion.WorkflowActivationCompletion"

  def workflow_activation_from_bytes(bytes) when is_binary(bytes) do
    with {:ok, proto} <- decode(@workflow_activation, bytes),
         {:ok, activation} <- workflow_activation_from_proto(proto) do
      {:ok, activation}
    end
  end

  def activity_task_from_bytes(bytes) when is_binary(bytes) do
    with {:ok, proto} <- decode(@activity_task, bytes),
         {:ok, task} <- activity_task_from_proto(proto) do
      {:ok, task}
    end
  end

  def workflow_completion_to_bytes(%Completion{} = completion, opts) do
    task_queue = Keyword.fetch!(opts, :task_queue)

    with {:ok, proto} <- workflow_completion_to_proto(completion, task_queue),
         {:ok, bytes} <- encode(@workflow_activation_completion, proto) do
      {:ok, bytes}
    end
  end

  def activity_completion_to_bytes(%ActivityCompletion{} = completion) do
    with {:ok, proto} <- activity_completion_to_proto(completion),
         {:ok, bytes} <- encode(@activity_task_completion, proto) do
      {:ok, bytes}
    end
  end

  def activity_heartbeat_to_bytes(task_token, details) when is_binary(task_token) do
    proto = %{
      task_token: task_token,
      details: if(is_nil(details), do: [], else: [PayloadConverter.term_to_payload(details)])
    }

    encode(@activity_heartbeat, proto)
  end

  defp encode(message, proto) do
    case Schema.encode(message, proto) do
      {:ok, iodata} -> {:ok, IO.iodata_to_binary(iodata)}
      {:error, error} -> {:error, format_error(error)}
    end
  rescue
    error -> {:error, Exception.message(error)}
  end

  defp decode(message, bytes) do
    case Schema.decode(message, bytes) do
      {:ok, proto} -> {:ok, proto}
      {:error, error} -> {:error, format_error(error)}
    end
  rescue
    error -> {:error, Exception.message(error)}
  end

  defp workflow_activation_from_proto(proto) do
    with {:ok, jobs} <- activation_jobs_from_proto(Map.get(proto, :jobs, [])) do
      {:ok,
       %Temporalex.Core.Activation{
         run_id: Map.get(proto, :run_id),
         timestamp: timestamp_to_datetime(Map.get(proto, :timestamp)),
         is_replaying: Map.get(proto, :is_replaying, false),
         history_length: Map.get(proto, :history_length, 0),
         history_size_bytes: Map.get(proto, :history_size_bytes),
         continue_as_new_suggested: Map.get(proto, :continue_as_new_suggested, false),
         available_internal_flags: Map.get(proto, :available_internal_flags, []),
         deployment_version: nil,
         jobs: jobs
       }}
    end
  end

  defp activation_jobs_from_proto(jobs) when is_list(jobs) do
    Enum.reduce_while(jobs, {:ok, []}, fn job, {:ok, acc} ->
      case activation_job_from_proto(job) do
        {:ok, job} -> {:cont, {:ok, [job | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, jobs} -> {:ok, Enum.reverse(jobs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp activation_job_from_proto(%{variant: {:initialize_workflow, init}}) do
    workflow_info = %{
      workflow_id: Map.get(init, :workflow_id, ""),
      workflow_type: Map.get(init, :workflow_type, ""),
      attempt: Map.get(init, :attempt, 0),
      identity: Map.get(init, :identity, ""),
      run_id: Map.get(init, :first_execution_run_id, "")
    }

    with {:ok, arguments} <- payloads_to_terms(Map.get(init, :arguments, [])),
         {:ok, headers} <- PayloadConverter.payload_map_to_term(Map.get(init, :headers, %{})) do
      {:ok,
       %Temporalex.Core.Job.InitializeWorkflow{
         workflow_type: Map.get(init, :workflow_type, ""),
         workflow_id: Map.get(init, :workflow_id, ""),
         arguments: arguments,
         headers: headers,
         workflow_info: workflow_info,
         randomness_seed: Map.get(init, :randomness_seed, 0)
       }}
    end
  end

  defp activation_job_from_proto(%{variant: {:fire_timer, timer}}) do
    {:ok, %Temporalex.Core.Job.TimerFired{seq: Map.get(timer, :seq, 0)}}
  end

  defp activation_job_from_proto(%{variant: {:resolve_activity, resolution}}) do
    with {:ok, result} <- activity_resolution_from_proto(Map.get(resolution, :result)) do
      {:ok,
       %Temporalex.Core.Job.ActivityResolved{
         seq: Map.get(resolution, :seq, 0),
         result: result
       }}
    end
  end

  defp activation_job_from_proto(%{variant: {:update_random_seed, seed}}) do
    {:ok,
     %Temporalex.Core.Job.UpdateRandomSeed{
       randomness_seed: Map.get(seed, :randomness_seed, 0)
     }}
  end

  defp activation_job_from_proto(%{variant: {:query_workflow, query}}) do
    with {:ok, args} <- payloads_to_terms(Map.get(query, :arguments, [])),
         {:ok, headers} <- PayloadConverter.payload_map_to_term(Map.get(query, :headers, %{})) do
      {:ok,
       %Temporalex.Core.Job.QueryReceived{
         query_id: Map.get(query, :query_id, ""),
         query_type: Map.get(query, :query_type, ""),
         args: args,
         headers: headers
       }}
    end
  end

  defp activation_job_from_proto(%{variant: {:cancel_workflow, cancel}}) do
    {:ok, %Temporalex.Core.Job.CancelWorkflow{reason: Map.get(cancel, :reason, "")}}
  end

  defp activation_job_from_proto(%{variant: {:signal_workflow, signal}}) do
    with {:ok, args} <- payloads_to_terms(Map.get(signal, :input, [])),
         {:ok, headers} <- PayloadConverter.payload_map_to_term(Map.get(signal, :headers, %{})) do
      {:ok,
       %Temporalex.Core.Job.SignalReceived{
         name: Map.get(signal, :signal_name, ""),
         args: args,
         headers: headers,
         identity: Map.get(signal, :identity, "")
       }}
    end
  end

  defp activation_job_from_proto(%{variant: {:notify_has_patch, patch}}) do
    {:ok, %Temporalex.Core.Job.NotifyPatch{id: Map.get(patch, :patch_id, "")}}
  end

  defp activation_job_from_proto(%{variant: {:do_update, update}}) do
    with {:ok, args} <- payloads_to_terms(Map.get(update, :input, [])),
         {:ok, headers} <- PayloadConverter.payload_map_to_term(Map.get(update, :headers, %{})) do
      {:ok,
       %Temporalex.Core.Job.UpdateReceived{
         id: Map.get(update, :id, ""),
         protocol_instance_id: Map.get(update, :protocol_instance_id, ""),
         name: Map.get(update, :name, ""),
         args: args,
         headers: headers,
         meta: nil,
         run_validator: Map.get(update, :run_validator, false)
       }}
    end
  end

  defp activation_job_from_proto(%{variant: {:remove_from_cache, eviction}}) do
    {:ok,
     %Temporalex.Core.Job.RemoveFromCache{
       reason: eviction_reason_to_atom(Map.get(eviction, :reason, :UNSPECIFIED)),
       message: Map.get(eviction, :message, "")
     }}
  end

  defp activation_job_from_proto(%{variant: {variant, _value}}) do
    {:error, "unsupported workflow activation job from Temporal Core: #{variant}"}
  end

  defp activation_job_from_proto(_job), do: {:error, "activation job had no variant"}

  defp activity_resolution_from_proto(%{status: {:completed, success}}) do
    case Map.fetch(success, :result) do
      {:ok, payload} ->
        with {:ok, term} <- PayloadConverter.payload_to_term(payload), do: {:ok, {:ok, term}}

      :error ->
        {:error, "activity result missing payload"}
    end
  end

  defp activity_resolution_from_proto(%{status: {:failed, failure}}) do
    with {:ok, failure} <- failure_from_proto(Map.get(failure, :failure)) do
      {:ok, {:error, failure}}
    end
  end

  defp activity_resolution_from_proto(%{status: {:cancelled, cancellation}}) do
    with {:ok, failure} <- failure_from_proto(Map.get(cancellation, :failure)) do
      {:ok, {:cancelled, failure}}
    end
  end

  defp activity_resolution_from_proto(%{status: {:backoff, backoff}}) do
    {:ok,
     {:backoff,
      %{
        attempt: Map.get(backoff, :attempt, 0),
        timeout: duration_to_millis(Map.get(backoff, :backoff_duration))
      }}}
  end

  defp activity_resolution_from_proto(nil), do: {:error, "activity resolution missing result"}
  defp activity_resolution_from_proto(_resolution), do: {:error, "activity resolution empty"}

  defp activity_task_from_proto(%{task_token: task_token, variant: {:start, start}}) do
    execution = Map.get(start, :workflow_execution, %{})

    with {:ok, input} <- payloads_to_terms(Map.get(start, :input, [])),
         {:ok, headers} <-
           PayloadConverter.payload_map_to_term(Map.get(start, :header_fields, %{})) do
      {:ok,
       %Temporalex.Core.ActivityTask{
         task_token: task_token,
         activity_id: Map.get(start, :activity_id, ""),
         activity_type: Map.get(start, :activity_type, ""),
         workflow_id: Map.get(execution, :workflow_id, ""),
         run_id: Map.get(execution, :run_id, ""),
         workflow_type: Map.get(start, :workflow_type, ""),
         namespace: Map.get(start, :workflow_namespace, ""),
         task_queue: nil,
         input: input,
         attempt: Map.get(start, :attempt, 0),
         heartbeat_timeout: nullable_duration_millis(Map.get(start, :heartbeat_timeout)),
         is_local: Map.get(start, :is_local, false),
         headers: headers,
         variant: :start,
         cancel_reason: nil
       }}
    end
  end

  defp activity_task_from_proto(%{task_token: task_token, variant: {:cancel, cancel}}) do
    {:ok,
     %Temporalex.Core.ActivityTask{
       task_token: task_token,
       variant: :cancel,
       cancel_reason: activity_cancel_reason(Map.get(cancel, :reason, :NOT_FOUND))
     }}
  end

  defp activity_task_from_proto(%{variant: {variant, _value}}) do
    {:error, "unsupported activity task from Temporal Core: #{variant}"}
  end

  defp activity_task_from_proto(_task), do: {:error, "activity task had no variant"}

  defp failure_from_proto(nil), do: {:ok, nil}

  defp failure_from_proto(failure) do
    with {:ok, cause} <- failure_from_proto(Map.get(failure, :cause)) do
      case Map.get(failure, :failure_info) do
        {:application_failure_info, info} ->
          with {:ok, details} <- payloads_to_terms_option(Map.get(info, :details)) do
            {:ok,
             %Failure.ApplicationError{
               message: Map.get(failure, :message, ""),
               source: Map.get(failure, :source, ""),
               stack_trace: Map.get(failure, :stack_trace, ""),
               type: Map.get(info, :type, ""),
               details: details,
               retryable?: not Map.get(info, :non_retryable, false),
               cause: cause
             }}
          end

        {:canceled_failure_info, info} ->
          with {:ok, details} <- payloads_to_terms_option(Map.get(info, :details)) do
            {:ok,
             %Failure.CancelledError{
               message: Map.get(failure, :message, ""),
               source: Map.get(failure, :source, ""),
               stack_trace: Map.get(failure, :stack_trace, ""),
               identity: Map.get(info, :identity, ""),
               details: details,
               cause: cause
             }}
          end

        {:timeout_failure_info, info} ->
          with {:ok, last_heartbeat_details} <-
                 payloads_to_terms_option(Map.get(info, :last_heartbeat_details)) do
            {:ok,
             %Failure.TimeoutError{
               message: Map.get(failure, :message, ""),
               source: Map.get(failure, :source, ""),
               stack_trace: Map.get(failure, :stack_trace, ""),
               timeout_type: timeout_type_from_proto(Map.get(info, :timeout_type)),
               last_heartbeat_details: last_heartbeat_details,
               cause: cause
             }}
          end

        {:activity_failure_info, info} ->
          {:ok,
           %Failure.ActivityError{
             message: Map.get(failure, :message, ""),
             source: Map.get(failure, :source, ""),
             stack_trace: Map.get(failure, :stack_trace, ""),
             identity: Map.get(info, :identity, ""),
             activity_id: Map.get(info, :activity_id, ""),
             activity_type: get_in(info, [:activity_type, :name]) || "",
             retry_state: retry_state_from_proto(Map.get(info, :retry_state)),
             cause: cause
           }}

        {:child_workflow_execution_failure_info, info} ->
          execution = Map.get(info, :workflow_execution, %{})

          {:ok,
           %Failure.WorkflowExecutionError{
             message: Map.get(failure, :message, ""),
             source: Map.get(failure, :source, ""),
             stack_trace: Map.get(failure, :stack_trace, ""),
             namespace: Map.get(info, :namespace, ""),
             workflow_id: Map.get(execution, :workflow_id, ""),
             run_id: Map.get(execution, :run_id, ""),
             workflow_type: get_in(info, [:workflow_type, :name]) || "",
             retry_state: retry_state_from_proto(Map.get(info, :retry_state)),
             cause: cause
           }}

        other ->
          {:ok,
           %Failure.UnknownError{
             message: Map.get(failure, :message, ""),
             source: Map.get(failure, :source, ""),
             stack_trace: Map.get(failure, :stack_trace, ""),
             failure_type: failure_info_type(other),
             cause: cause
           }}
      end
    end
  end

  defp payloads_to_terms(payloads), do: PayloadConverter.payloads_to_terms(payloads || [])

  defp payloads_to_terms_option(nil), do: {:ok, []}
  defp payloads_to_terms_option(%{} = payloads) when map_size(payloads) == 0, do: {:ok, []}

  defp payloads_to_terms_option(%{payloads: payloads}) do
    payloads_to_terms(payloads)
  end

  defp timestamp_to_datetime(nil), do: nil

  defp timestamp_to_datetime(timestamp) do
    seconds = Map.get(timestamp, :seconds, 0)
    nanos = max(Map.get(timestamp, :nanos, 0), 0)

    seconds
    |> DateTime.from_unix!(:second)
    |> Map.put(:microsecond, {div(nanos, 1000), 6})
  rescue
    _error -> ~U[1970-01-01 00:00:00Z]
  end

  defp nullable_duration_millis(nil), do: nil
  defp nullable_duration_millis(duration), do: duration_to_millis(duration)

  defp duration_to_millis(nil), do: 0

  defp duration_to_millis(duration) do
    seconds = max(Map.get(duration, :seconds, 0), 0)
    nanos = max(Map.get(duration, :nanos, 0), 0)
    seconds * 1000 + div(nanos, 1_000_000)
  end

  defp eviction_reason_to_atom(:CACHE_FULL), do: :cache_full
  defp eviction_reason_to_atom(:CACHE_MISS), do: :cache_miss
  defp eviction_reason_to_atom(:NONDETERMINISM), do: :nondeterminism
  defp eviction_reason_to_atom(:LANG_FAIL), do: :lang_fail
  defp eviction_reason_to_atom(:LANG_REQUESTED), do: :lang_requested
  defp eviction_reason_to_atom(:TASK_NOT_FOUND), do: :task_not_found
  defp eviction_reason_to_atom(:UNHANDLED_COMMAND), do: :unhandled_command
  defp eviction_reason_to_atom(:FATAL), do: :fatal
  defp eviction_reason_to_atom(:PAGINATION_OR_HISTORY_FETCH), do: :pagination_or_history_fetch
  defp eviction_reason_to_atom(:WORKFLOW_EXECUTION_ENDING), do: :workflow_execution_ending
  defp eviction_reason_to_atom(_), do: :unspecified

  defp activity_cancel_reason(:CANCELLED), do: :cancelled
  defp activity_cancel_reason(:TIMED_OUT), do: :timeout
  defp activity_cancel_reason(:WORKER_SHUTDOWN), do: :shutdown
  defp activity_cancel_reason(_), do: :cancel

  defp retry_state_from_proto(:RETRY_STATE_IN_PROGRESS), do: :in_progress
  defp retry_state_from_proto(:RETRY_STATE_NON_RETRYABLE_FAILURE), do: :non_retryable_failure
  defp retry_state_from_proto(:RETRY_STATE_TIMEOUT), do: :timeout

  defp retry_state_from_proto(:RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED),
    do: :maximum_attempts_reached

  defp retry_state_from_proto(:RETRY_STATE_RETRY_POLICY_NOT_SET), do: :retry_policy_not_set
  defp retry_state_from_proto(:RETRY_STATE_INTERNAL_SERVER_ERROR), do: :internal_server_error
  defp retry_state_from_proto(:RETRY_STATE_CANCEL_REQUESTED), do: :cancel_requested
  defp retry_state_from_proto(_), do: :unspecified

  defp timeout_type_from_proto(:TIMEOUT_TYPE_START_TO_CLOSE), do: :start_to_close
  defp timeout_type_from_proto(:TIMEOUT_TYPE_SCHEDULE_TO_START), do: :schedule_to_start
  defp timeout_type_from_proto(:TIMEOUT_TYPE_SCHEDULE_TO_CLOSE), do: :schedule_to_close
  defp timeout_type_from_proto(:TIMEOUT_TYPE_HEARTBEAT), do: :heartbeat
  defp timeout_type_from_proto(_), do: :unspecified

  defp failure_info_type({:timeout_failure_info, _}), do: :timeout_failure
  defp failure_info_type({:canceled_failure_info, _}), do: :cancelled_failure
  defp failure_info_type({:terminated_failure_info, _}), do: :terminated_failure
  defp failure_info_type({:server_failure_info, _}), do: :server_failure
  defp failure_info_type({:reset_workflow_failure_info, _}), do: :reset_workflow_failure
  defp failure_info_type({:activity_failure_info, _}), do: :activity_failure
  defp failure_info_type({:child_workflow_execution_failure_info, _}), do: :child_workflow_failure

  defp failure_info_type({:nexus_operation_execution_failure_info, _}),
    do: :nexus_operation_failure

  defp failure_info_type({:nexus_handler_failure_info, _}), do: :nexus_handler_failure
  defp failure_info_type({:application_failure_info, _}), do: :failed
  defp failure_info_type(_), do: :unknown_failure

  defp workflow_completion_to_proto(
         %Completion{run_id: run_id, status: {:ok, commands}},
         task_queue
       ) do
    with {:ok, commands} <- commands_to_proto(commands, task_queue) do
      {:ok,
       %{
         run_id: run_id,
         status:
           {:successful,
            %{
              commands: commands,
              used_internal_flags: [],
              versioning_behavior: :VERSIONING_BEHAVIOR_UNSPECIFIED
            }}
       }}
    end
  end

  defp workflow_completion_to_proto(
         %Completion{run_id: run_id, status: {:failed, reason, opts}},
         _task_queue
       ) do
    {:ok,
     %{
       run_id: run_id,
       status:
         {:failed,
          %{
            failure: failure_to_proto(reason, "Temporalex activation failure"),
            force_cause: force_cause_from_opts(opts)
          }}
     }}
  end

  defp workflow_completion_to_proto(_completion, _task_queue) do
    {:error, "unsupported workflow completion status"}
  end

  defp commands_to_proto(commands, task_queue) when is_list(commands) do
    Enum.reduce_while(commands, {:ok, []}, fn command, {:ok, acc} ->
      case command_to_proto(command, task_queue) do
        {:ok, proto} -> {:cont, {:ok, [proto | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, commands} -> {:ok, Enum.reverse(commands)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp command_to_proto(%Command.StartTimer{seq: seq, duration_ms: duration_ms}, _task_queue) do
    with {:ok, duration} <- duration_from_ms(duration_ms, "timer duration") do
      {:ok, %{variant: {:start_timer, %{seq: seq, start_to_fire_timeout: duration}}}}
    end
  end

  defp command_to_proto(%Command.CancelTimer{seq: seq}, _task_queue) do
    {:ok, %{variant: {:cancel_timer, %{seq: seq}}}}
  end

  defp command_to_proto(%Command.RequestCancelActivity{seq: seq}, _task_queue) do
    {:ok, %{variant: {:request_cancel_activity, %{seq: seq}}}}
  end

  defp command_to_proto(%Command.ScheduleActivity{} = command, default_task_queue) do
    opts = command.opts || []

    with {:ok, timeout_ms} <- activity_timeout_ms(opts),
         {:ok, schedule_to_close_timeout} <-
           duration_from_opts(
             opts,
             [:schedule_to_close_timeout],
             timeout_ms,
             "activity schedule_to_close_timeout"
           ),
         {:ok, schedule_to_start_timeout} <-
           optional_duration_from_opts(
             opts,
             [:schedule_to_start_timeout],
             "activity schedule_to_start_timeout"
           ),
         {:ok, start_to_close_timeout} <-
           duration_from_ms(timeout_ms, "activity start_to_close_timeout"),
         {:ok, heartbeat_timeout} <-
           optional_duration_from_opts(opts, [:heartbeat_timeout], "activity heartbeat_timeout"),
         {:ok, headers} <- payload_map_from_opts(opts, :headers),
         {:ok, retry_policy} <- retry_policy_from_opts(opts),
         {:ok, cancellation_type} <- activity_cancellation_type_from_opts(opts) do
      schedule =
        compact(%{
          seq: command.seq,
          activity_id: command.activity_id,
          activity_type: command.type,
          task_queue: Keyword.get(opts, :task_queue, default_task_queue),
          headers: headers,
          arguments: PayloadConverter.term_to_payloads_list(command.input || []),
          schedule_to_close_timeout: schedule_to_close_timeout,
          schedule_to_start_timeout: schedule_to_start_timeout,
          start_to_close_timeout: start_to_close_timeout,
          heartbeat_timeout: heartbeat_timeout,
          retry_policy: retry_policy,
          cancellation_type: cancellation_type,
          do_not_eagerly_execute: false
        })

      {:ok, %{variant: {:schedule_activity, schedule}}}
    end
  end

  defp command_to_proto(%Command.CompleteWorkflow{result: result}, _task_queue) do
    {:ok,
     %{
       variant:
         {:complete_workflow_execution, %{result: PayloadConverter.term_to_payload(result)}}
     }}
  end

  defp command_to_proto(%Command.FailWorkflow{reason: reason}, _task_queue) do
    {:ok,
     %{
       variant:
         {:fail_workflow_execution,
          %{failure: failure_to_proto(reason, "Temporalex workflow failure")}}
     }}
  end

  defp command_to_proto(%Command.ContinueAsNew{} = command, _task_queue) do
    opts = command.opts || []

    with {:ok, workflow_run_timeout} <-
           optional_duration_from_opts(
             opts,
             [:run_timeout, :workflow_run_timeout],
             "duration option"
           ),
         {:ok, workflow_task_timeout} <-
           optional_duration_from_opts(
             opts,
             [:task_timeout, :workflow_task_timeout],
             "duration option"
           ),
         {:ok, memo} <- payload_map_from_opts(opts, :memo),
         {:ok, headers} <- payload_map_from_opts(opts, :headers),
         {:ok, search_attributes} <- search_attributes_from_opts(opts),
         {:ok, retry_policy} <- retry_policy_from_opts(opts),
         {:ok, versioning_intent} <- versioning_intent_from_opts(opts),
         {:ok, initial_versioning_behavior} <- continue_as_new_versioning_behavior_from_opts(opts) do
      continue =
        compact(%{
          workflow_type: command.workflow_type || "",
          task_queue: command.task_queue || "",
          arguments: [PayloadConverter.term_to_payload(command.input)],
          workflow_run_timeout: workflow_run_timeout,
          workflow_task_timeout: workflow_task_timeout,
          memo: memo,
          headers: headers,
          search_attributes: search_attributes,
          retry_policy: retry_policy,
          versioning_intent: versioning_intent,
          initial_versioning_behavior: initial_versioning_behavior
        })

      {:ok, %{variant: {:continue_as_new_workflow_execution, continue}}}
    end
  end

  defp command_to_proto(%Command.CancelWorkflow{}, _task_queue) do
    {:ok, %{variant: {:cancel_workflow_execution, %{}}}}
  end

  defp command_to_proto(%Command.RespondToQuery{} = command, _task_queue) do
    with {:ok, result} <- query_result_to_proto(command.result) do
      {:ok,
       %{
         variant:
           {:respond_to_query,
            %{
              query_id: command.query_id,
              variant: result
            }}
       }}
    end
  end

  defp command_to_proto(%Command.RespondToUpdate{} = command, _task_queue) do
    with {:ok, response} <- update_response_to_proto(command.response) do
      {:ok,
       %{
         variant:
           {:update_response,
            %{
              protocol_instance_id: command.protocol_instance_id,
              response: response
            }}
       }}
    end
  end

  defp command_to_proto(%Command.SetPatchMarker{} = command, _task_queue) do
    {:ok,
     %{
       variant:
         {:set_patch_marker,
          %{
            patch_id: command.id,
            deprecated: command.deprecated
          }}
     }}
  end

  defp command_to_proto(%Command.UpsertSearchAttributes{attrs: attrs}, _task_queue) do
    with {:ok, indexed_fields} <- PayloadConverter.search_attributes_to_payload_map(attrs) do
      {:ok,
       %{
         variant:
           {:upsert_workflow_search_attributes,
            %{search_attributes: %{indexed_fields: indexed_fields}}}
       }}
    end
  end

  defp command_to_proto(command, _task_queue) do
    {:error, "unsupported workflow command #{inspect(command.__struct__)}"}
  end

  defp activity_completion_to_proto(%ActivityCompletion{task_token: task_token, result: result}) do
    with {:ok, status} <- activity_result_to_proto(result) do
      {:ok, %{task_token: task_token, result: %{status: status}}}
    end
  end

  defp activity_result_to_proto({:ok, value}) do
    {:ok, {:completed, %{result: PayloadConverter.term_to_payload(value)}}}
  end

  defp activity_result_to_proto({:error, reason}) do
    {:ok, {:failed, %{failure: failure_to_proto(reason, "Temporalex activity failure")}}}
  end

  defp activity_result_to_proto({:cancelled, reason}) do
    {:ok, {:cancelled, %{failure: cancelled_failure_to_proto(reason)}}}
  end

  defp activity_result_to_proto(_result) do
    {:error, "activity completion result must be a tagged tuple"}
  end

  defp query_result_to_proto({:ok, value}) do
    {:ok, {:succeeded, %{response: PayloadConverter.term_to_payload(value)}}}
  end

  defp query_result_to_proto({:error, reason}) do
    {:ok, {:failed, failure_to_proto(reason, "Temporalex query failure")}}
  end

  defp query_result_to_proto(_result) do
    {:error, "query result must be {:ok, value} or {:error, reason}"}
  end

  defp update_response_to_proto(:accepted), do: {:ok, {:accepted, %{}}}

  defp update_response_to_proto({:completed, value}) do
    {:ok, {:completed, PayloadConverter.term_to_payload(value)}}
  end

  defp update_response_to_proto({:rejected, reason}) do
    {:ok, {:rejected, failure_to_proto(reason, "Temporalex update rejected")}}
  end

  defp update_response_to_proto(_response), do: {:error, "unsupported update response"}

  defp failure_to_proto({:exception, error, _stacktrace}, default_message) do
    failure_to_proto(error, default_message)
  end

  defp failure_to_proto(%Failure.ApplicationError{} = error, default_message) do
    compact(%{
      message: non_empty(error.message, default_message),
      source: error.source || "Temporalex",
      stack_trace: error.stack_trace || "",
      cause: maybe_failure(error.cause, "Temporalex caused failure"),
      failure_info:
        {:application_failure_info,
         %{
           type: non_empty(error.type, "Temporalex.ApplicationError"),
           non_retryable: not error.retryable?,
           details: %{payloads: PayloadConverter.term_to_payloads_list(error.details || [])}
         }}
    })
  end

  defp failure_to_proto(%Failure.CancelledError{} = error, _default_message) do
    compact(%{
      message: error.message || "Temporalex activity cancelled",
      source: error.source || "Temporalex",
      stack_trace: error.stack_trace || "",
      cause: maybe_failure(error.cause, "Temporalex caused failure"),
      failure_info:
        {:canceled_failure_info,
         compact(%{
           details: %{payloads: PayloadConverter.term_to_payloads_list(error.details || [])},
           identity: error.identity || ""
         })}
    })
  end

  defp failure_to_proto(%Failure.TimeoutError{} = error, default_message) do
    compact(%{
      message: non_empty(error.message, default_message),
      source: error.source || "Temporalex",
      stack_trace: error.stack_trace || "",
      cause: maybe_failure(error.cause, "Temporalex caused failure"),
      failure_info:
        {:timeout_failure_info,
         %{
           timeout_type: timeout_type_to_proto(error.timeout_type),
           last_heartbeat_details: %{
             payloads: PayloadConverter.term_to_payloads_list(error.last_heartbeat_details || [])
           }
         }}
    })
  end

  defp failure_to_proto(%Failure.ActivityError{} = error, default_message) do
    compact(%{
      message: non_empty(error.message, default_message),
      source: error.source || "Temporalex",
      stack_trace: error.stack_trace || "",
      cause: maybe_failure(error.cause, "Temporalex caused failure"),
      failure_info:
        {:activity_failure_info,
         compact(%{
           identity: error.identity || "",
           activity_type:
             if(blank?(error.activity_type), do: nil, else: %{name: error.activity_type}),
           activity_id: error.activity_id || "",
           retry_state: retry_state_to_proto(error.retry_state)
         })}
    })
  end

  defp failure_to_proto(%Failure.WorkflowExecutionError{} = error, default_message) do
    compact(%{
      message: non_empty(error.message, default_message),
      source: error.source || "Temporalex",
      stack_trace: error.stack_trace || "",
      cause: maybe_failure(error.cause, "Temporalex caused failure"),
      failure_info:
        {:child_workflow_execution_failure_info,
         compact(%{
           namespace: error.namespace || "",
           workflow_execution:
             if(blank?(error.workflow_id) and blank?(error.run_id),
               do: nil,
               else: %{workflow_id: error.workflow_id || "", run_id: error.run_id || ""}
             ),
           workflow_type:
             if(blank?(error.workflow_type), do: nil, else: %{name: error.workflow_type}),
           retry_state: retry_state_to_proto(error.retry_state)
         })}
    })
  end

  defp failure_to_proto(term, default_message) do
    message =
      if is_binary(term) and term != "" do
        term
      else
        default_message
      end

    %{
      message: message,
      source: "Temporalex",
      failure_info:
        {:application_failure_info,
         %{
           type: "Temporalex.ApplicationError",
           non_retryable: false,
           details: %{payloads: [PayloadConverter.term_to_payload(term)]}
         }}
    }
  end

  defp cancelled_failure_to_proto(%Failure.CancelledError{} = error),
    do: failure_to_proto(error, error.message)

  defp cancelled_failure_to_proto(reason) do
    %{
      message: "Temporalex activity cancelled",
      source: "Temporalex",
      failure_info:
        {:canceled_failure_info,
         %{details: %{payloads: [PayloadConverter.term_to_payload(reason)]}}}
    }
  end

  defp maybe_failure(nil, _message), do: nil
  defp maybe_failure(cause, message), do: failure_to_proto(cause, message)

  defp activity_timeout_ms(opts) do
    timeout =
      Keyword.get(opts, :timeout) ||
        Keyword.get(opts, :start_to_close_timeout) ||
        60_000

    non_negative_millis(timeout, "activity timeout")
  end

  defp duration_from_opts(opts, keys, default_ms, option_name) do
    case find_option(opts, keys) do
      nil -> duration_from_ms(default_ms, option_name)
      ms -> duration_from_ms(ms, option_name)
    end
  end

  defp optional_duration_from_opts(opts, keys, option_name) do
    case find_option(opts, keys) do
      nil -> {:ok, nil}
      ms -> duration_from_ms(ms, option_name)
    end
  end

  defp duration_from_ms(ms, option_name) do
    with {:ok, ms} <- non_negative_millis(ms, option_name) do
      {:ok, %{seconds: div(ms, 1000), nanos: rem(ms, 1000) * 1_000_000}}
    end
  end

  defp non_negative_millis(ms, _option_name) when is_integer(ms) and ms >= 0, do: {:ok, ms}

  defp non_negative_millis(ms, option_name) when is_integer(ms),
    do: {:error, "#{option_name} must be non-negative"}

  defp non_negative_millis(_ms, option_name),
    do: {:error, "#{option_name} must be an integer number of milliseconds"}

  defp payload_map_from_opts(opts, key) do
    opts
    |> Keyword.get(key)
    |> PayloadConverter.term_to_payload_map()
  end

  defp search_attributes_from_opts(opts) do
    case Keyword.fetch(opts, :search_attributes) do
      :error ->
        {:ok, nil}

      {:ok, nil} ->
        {:ok, nil}

      {:ok, attrs} ->
        with {:ok, indexed_fields} <- PayloadConverter.search_attributes_to_payload_map(attrs) do
          {:ok, %{indexed_fields: indexed_fields}}
        end
    end
  end

  defp retry_policy_from_opts(opts) do
    case Keyword.fetch(opts, :retry_policy) do
      :error -> {:ok, nil}
      {:ok, nil} -> {:ok, nil}
      {:ok, retry_policy} -> retry_policy_to_proto(retry_policy)
    end
  end

  defp retry_policy_to_proto(opts) when is_list(opts) do
    with {:ok, initial_interval} <-
           optional_duration_from_opts(opts, [:initial_interval], "retry_policy.initial_interval"),
         {:ok, maximum_interval} <-
           optional_duration_from_opts(opts, [:maximum_interval], "retry_policy.maximum_interval"),
         {:ok, backoff_coefficient} <- backoff_coefficient_from_opts(opts),
         {:ok, maximum_attempts} <- maximum_attempts_from_opts(opts) do
      {:ok,
       compact(%{
         initial_interval: initial_interval,
         backoff_coefficient: backoff_coefficient,
         maximum_interval: maximum_interval,
         maximum_attempts: maximum_attempts,
         non_retryable_error_types: Keyword.get(opts, :non_retryable_error_types, [])
       })}
    end
  end

  defp retry_policy_to_proto(_opts), do: {:error, "retry_policy must be a keyword list"}

  defp backoff_coefficient_from_opts(opts) do
    case Keyword.get(opts, :backoff_coefficient) do
      nil ->
        {:ok, 0.0}

      value when is_number(value) and value >= 1.0 ->
        {:ok, value * 1.0}

      value when is_number(value) ->
        {:error, "retry_policy.backoff_coefficient must be 1.0 or larger"}

      _value ->
        {:error, "retry_policy.backoff_coefficient must be numeric"}
    end
  end

  defp maximum_attempts_from_opts(opts) do
    attempts = Keyword.get(opts, :maximum_attempts, 0)

    cond do
      not is_integer(attempts) ->
        {:error, "retry_policy.maximum_attempts must fit in a non-negative i32"}

      attempts < 0 or attempts > 2_147_483_647 ->
        {:error, "retry_policy.maximum_attempts must fit in a non-negative i32"}

      true ->
        {:ok, attempts}
    end
  end

  defp activity_cancellation_type_from_opts(opts) do
    case Keyword.get(opts, :cancellation_type, :wait_cancellation_completed) do
      :try_cancel -> {:ok, :TRY_CANCEL}
      :wait_cancellation_completed -> {:ok, :WAIT_CANCELLATION_COMPLETED}
      :abandon -> {:ok, :ABANDON}
      _ -> {:error, "unsupported activity cancellation type"}
    end
  end

  defp versioning_intent_from_opts(opts) do
    case Keyword.get(opts, :versioning_intent, :unspecified) do
      nil -> {:ok, :UNSPECIFIED}
      :unspecified -> {:ok, :UNSPECIFIED}
      :compatible -> {:ok, :COMPATIBLE}
      :default -> {:ok, :DEFAULT}
      _ -> {:error, "unsupported continue-as-new versioning_intent"}
    end
  end

  defp continue_as_new_versioning_behavior_from_opts(opts) do
    case Keyword.get(opts, :initial_versioning_behavior, :unspecified) do
      nil -> {:ok, :CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_UNSPECIFIED}
      :unspecified -> {:ok, :CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_UNSPECIFIED}
      :auto_upgrade -> {:ok, :CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE}
      :use_ramping_version -> {:ok, :CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_USE_RAMPING_VERSION}
      _ -> {:error, "unsupported continue-as-new initial_versioning_behavior"}
    end
  end

  defp retry_state_to_proto(:in_progress), do: :RETRY_STATE_IN_PROGRESS
  defp retry_state_to_proto(:non_retryable_failure), do: :RETRY_STATE_NON_RETRYABLE_FAILURE
  defp retry_state_to_proto(:timeout), do: :RETRY_STATE_TIMEOUT
  defp retry_state_to_proto(:maximum_attempts_reached), do: :RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
  defp retry_state_to_proto(:retry_policy_not_set), do: :RETRY_STATE_RETRY_POLICY_NOT_SET
  defp retry_state_to_proto(:internal_server_error), do: :RETRY_STATE_INTERNAL_SERVER_ERROR
  defp retry_state_to_proto(:cancel_requested), do: :RETRY_STATE_CANCEL_REQUESTED
  defp retry_state_to_proto(_), do: :RETRY_STATE_UNSPECIFIED

  defp timeout_type_to_proto(:start_to_close), do: :TIMEOUT_TYPE_START_TO_CLOSE
  defp timeout_type_to_proto(:schedule_to_start), do: :TIMEOUT_TYPE_SCHEDULE_TO_START
  defp timeout_type_to_proto(:schedule_to_close), do: :TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
  defp timeout_type_to_proto(:heartbeat), do: :TIMEOUT_TYPE_HEARTBEAT
  defp timeout_type_to_proto(_), do: :TIMEOUT_TYPE_UNSPECIFIED

  defp force_cause_from_opts(opts) do
    case Keyword.get(opts, :force_cause) do
      :nondeterminism -> :WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR
      _ -> :WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED
    end
  end

  defp find_option(opts, keys) do
    Enum.find_value(keys, fn key ->
      case Keyword.fetch(opts, key) do
        {:ok, nil} -> nil
        {:ok, value} -> value
        :error -> nil
      end
    end)
  end

  defp non_empty(value, _default) when is_binary(value) and value != "", do: value
  defp non_empty(_value, default), do: default

  defp blank?(nil), do: true
  defp blank?(""), do: true
  defp blank?(_), do: false

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp format_error(%MiniPB.Error{} = error), do: Exception.message(error)
  defp format_error(reason) when is_binary(reason), do: reason
  defp format_error(reason), do: inspect(reason)
end
