defmodule Perhap.Adapters.Eventstore.Dynamo do
  @moduledoc """
  Dynamo Adapter for perhap.

  This is a singleton instance of the eventstore.  It receives events from calling
  processes through the put_event/1 interface.  It stores the event locally and
  returns control to the calling process immediately.

  Approximately every 100ms (configurable) it takes all the events that have been
  saved up by put_event and creates new processes to submit those events to Dynamo.

  The Dynamo instance is configured in the application config (usually config.exs)
  where the event table, index table, batching interval parts of this adapter and
  the aws credentials and region can be specified for the supporting library ExAws

  There are two ways of getting events, get_event/1 which takes an event_id and
  returns only 1 event matching that id, and get_event/2 which takes a context atom
  and two options :entity_id and :after for narrowing the results.

  Both get_event and get_events check the local events before contacting Dynamo
  for additional results.
  """
  use Perhap.Adapters.Eventstore
  use GenServer

  @type t :: [ events: events, index: indexes ]
  @type events  :: %{ required(Perhap.Event.UUIDv1.t) => Perhap.Event.t }
  @type indexes :: %{ required({atom(), Perhap.Event.UUIDv4.t}) => list(Perhap.Event.UUIDv1.t) }

  @event_table Application.get_env(:perhap_dynamo, :event_table_name, "Events")
  @index_table Application.get_env(:perhap_dynamo, :event_index_table_name, "Index")
  @batch_write_interval Application.get_env(:perhap_dynamo, :batch_write_interval, 100)


  ### Interface
  @spec start_link(opts: any()) ::   {:ok, pid} | :ignore | {:error, {:already_started, pid} | term}
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: :eventstore)
  end

  @doc """
  Put event saves an event within the local process and returns control to the
  calling process. This adapter treats events collected locally as saved to Dynamo.

  ## Example:

  >Perhap.Adapters.Eventstore.Dynamo.put_event(%Perhap.Event{})
  :ok

  """
  @spec put_event(event: Perhap.Event.t) :: :ok | {:error, term}
  def put_event(event) do
    GenServer.call(:eventstore, {:put_event, event})
  end

  @doc """
  Retrieves an event using its event_id. Checks the locally stored events before
  going to Dynamo for the event.

  ## Example:
  {:ok, %Perhap.Event{}} = Perhap.Adapters.Eventstore.Dynamo.get_event(event_id)
  """
  @spec get_event(event_id: Perhap.Event.UUIDv1) :: {:ok, Perhap.Event.t} | {:error, term}
  def get_event(event_id) do
    GenServer.call(:eventstore, {:get_event, event_id})
  end

  @doc """
  Retrieves events based on the context associated with those events.  The context
  can be found in the event metadata (Perhap.Event.Metadata).  The results can be
  narrowed by supplying an entity_id as an option or an after option which will only
  retrieve events created after the supplied event_id.

  Returns a list of events inside an :ok tuple response ({:ok, events}) or empty
  {:ok, []} if no events were found.  Can return an :error tuple (:error, reason)
  if an unknown error occurred.

  ## Example:

  {:ok, events} = get_events(event.metadata.context) #get all events with the same context as this event
  {:ok []} = get_Events(:no_events_with_this_context)
  {:ok, events} = get_events(event.metadata.context, [entity_id: event.metadata.entity_id])
  {:ok, events} = get_events(event.metadata.context, [after: event.event_id])

  """
  
  @spec get_events(atom(), [entity_id: Perhap.Event.UUIDv4.t, after: Perhap.Event.UUIDv1.t]) :: {:ok, list(Perhap.Event.t)} | {:error, term}
  def get_events(context, opts \\ []) do
    GenServer.call(:eventstore, {:get_events, context, opts})
  end

  ### Server
  def init(_args) do
    interval = @batch_write_interval
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:ok, %{pending: [], posting: %{}}}
  end

  def handle_call({:put_event, event}, _from, events) do
    {:reply, :ok, %{events | pending: [event | events.pending]}}
  end

  def handle_call(:put_complete, {pid, _tag}, events = %{posting: posting}) do
    :ok = write_index(Map.get(posting, pid))
    {:reply, :received, %{events | posting: Map.delete(posting, pid)}}
  end

  def handle_call({:get_event, event_id}, _from, events) do
    result = case check_pending_events(event_id, events) do
      {:ok, event} ->
        {:ok, event}
      {:error, _reason} ->
        event_id_time_order = event_id |> Perhap.Event.uuid_v1_to_time_order
        dynamo_object = ExAws.Dynamo.get_item(@event_table, %{event_id: event_id_time_order})
        |> ExAws.request!

        case dynamo_object do
          %{"Item" => result} ->
            metadata = ExAws.Dynamo.decode_item(Map.get(result, "metadata"), as: Perhap.Event.Metadata)
            metadata = %Perhap.Event.Metadata{metadata | context: String.to_atom(metadata.context), type: String.to_atom(metadata.type)}

            event = ExAws.Dynamo.decode_item(dynamo_object, as: Perhap.Event)

            {:ok, %Perhap.Event{event | event_id: metadata.event_id, metadata: metadata}}
          %{} ->
            {:error, "Event not found"}
        end
    end
    {:reply, result, events}
  end

  def handle_call({:get_events, context, opts}, _from, event_state) do
    event_ids = case Keyword.has_key?(opts, :entity_id) do
      true ->
        dynamo_object = ExAws.Dynamo.get_item(@index_table, %{context: context, entity_id: opts[:entity_id]})
        |> ExAws.request!

        from_dynamo = case dynamo_object do
          %{"Item" => data} ->
            ExAws.Dynamo.Decoder.decode(data)
            |> Map.get("events", [])
          %{} ->
            []
        end
        from_dynamo ++ get_by_context(event_state, context, opts[:entity_id])
      _ ->
        get_by_context(event_state, context, nil) ++ ExAws.Dynamo.query(@index_table,
                            expression_attribute_values: [context: context],
                            key_condition_expression: "context = :context")
                        |> ExAws.request!
                        |> Map.get("Items")
                        |> Enum.map(fn x -> ExAws.Dynamo.Decoder.decode(x) end)
                        |> Enum.map(fn x -> Map.get(x, "events") end)
                        |> List.flatten

    end

    if event_ids == [] do
      {:reply, {:ok, []}, event_state}
    else
      event_ids2 = case Keyword.has_key?(opts, :after) do
        true ->
          after_event = time_order(opts[:after])
          event_ids |> Enum.filter(fn ev -> ev > after_event end)
        _ -> event_ids
      end

      event_ids3 = for event_id <- event_ids2, do: [event_id: event_id]

      events = Enum.chunk_every(event_ids3, 100) |> batch_get([])

      {:reply, {:ok, events}, event_state}
    end
  end

  def handle_info({:batch_write, interval}, events = %{pending: []}) do
    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, events}
  end

  def handle_info({:batch_write, interval}, events) do
    chunked = events.pending
    |> Enum.chunk_every(25)
    |> Enum.map(fn chunk -> with {:ok, pid} <- Task.start(__MODULE__, :put_to_dynamo, [chunk])
                            do {pid, chunk}
                            else err -> raise err
                            end
                end)
    posted = Enum.reduce(chunked, events.posting, fn {pid, chunk}, acc -> Map.put(acc, pid, chunk) end)

    Process.send_after(self(), {:batch_write, interval}, interval)
    {:noreply, %{pending: [], posting: posted}}
  end

  ### Helpers

  defp check_pending_events(event_id, %{pending: pending, posting: posting}) do
    case Enum.find(pending, fn (event) -> event.event_id == event_id end) do
      nil ->
        check_posting_events(event_id, posting)
      event ->
        {:ok, event}
    end
  end

  defp check_posting_events(event_id, posting) do
    case Map.values(posting) |> List.flatten |> Enum.find(fn (event) -> event.event_id == event_id end) do
      nil ->
        {:error, "Event not found"}
      event ->
        {:ok, event}
    end
  end

  defp get_by_context(%{pending: pending, posting: posting}, context, nil) do
    (Enum.filter(pending, fn event -> event.metadata.context == context end) |> Enum.map(fn event -> event.event_id end))
    ++ (Map.values(posting) |> List.flatten |> Enum.filter(fn event -> event.metadata.context == context end) |> Enum.map(fn event -> event.event_id end))
  end

  defp get_by_context(%{pending: pending, posting: posting}, context, entity_id) do
    (Enum.filter(pending, fn event -> event.metadata.context == context and event.metadata.entity_id == entity_id end) |> Enum.map(fn event -> event.event_id end))
    ++ (Map.values(posting) |> List.flatten |> Enum.filter(fn event -> event.metadata.context == context and event.metadata.entity_id == entity_id end) |> Enum.map(fn event -> event.event_id end))
  end



  defp batch_get([], events) do
    events
  end

  defp batch_get([chunk | rest], event_accumulator) do
    events = ExAws.Dynamo.batch_get_item(%{@event_table => [keys: chunk]})
             |> ExAws.request!
             |> Map.get("Responses")
             |> Map.get("Events")
             |> Enum.map(fn event -> {event, ExAws.Dynamo.decode_item(event["metadata"], as: Perhap.Event.Metadata)} end)
             |> Enum.map(fn {event, metadata} ->
               %Perhap.Event{ExAws.Dynamo.decode_item(event, as: Perhap.Event)
               | event_id: metadata.event_id,
                 metadata: %Perhap.Event.Metadata{metadata | context: String.to_atom(metadata.context),
                                                             type: String.to_atom(metadata.type)}} end)

    batch_get(rest, event_accumulator ++ events)
  end

  defp time_order(maybe_uuidv1) do
    case Perhap.Event.is_time_order?(maybe_uuidv1) do
      true -> maybe_uuidv1
      _ -> maybe_uuidv1 |> Perhap.Event.uuid_v1_to_time_order
    end
  end

  #defp decode_data(data) do
  #  Enum.reduce(data, %{}, fn({key, value}, map) ->
  #    Map.put(map, String.to_atom(key), value) end)
  #end

  def put_to_dynamo(events) do
    events = events
             |> Enum.map(fn event -> %Perhap.Event{event | event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order,
                                                           metadata: Map.from_struct(event.metadata)}
                                     |> Map.from_struct end)
    :ok = batch_put(events)
    GenServer.call(:eventstore, :put_complete)
  end

  defp batch_put([]) do
    :ok
  end

  defp batch_put(events) do
    event_put_request = events
                        |> Enum.map(fn event -> [put_request:
                                                  [item: event]] end)
    do_write_events(events, event_put_request)
  end

  defp process_index([], index) do
    index
    |> Map.keys
    |> Enum.map(fn {context, entity_id} -> [put_request:
                                             [item: %{context: context,
                                                      entity_id: entity_id,
                                                      events: Map.get(index, {context, entity_id})}]] end)
  end

  defp process_index([event | rest], index) do
    index_key = {event.metadata.context, event.metadata.entity_id}
    indexed_events = [event.event_id | Map.get(index, index_key, [])]
    process_index(rest, Map.put(index, index_key, indexed_events))
  end

  defp make_index_keys(events) do
    events
    |> Enum.map(fn event -> %{context: event.metadata.context,
                              entity_id: event.metadata.entity_id} end)
    |> Enum.dedup
  end

  defp retrieve_index(index_keys) do
    ExAws.Dynamo.batch_get_item(%{@index_table => [keys: index_keys]})
              |> ExAws.request!
              |> Map.get("Responses")
              |> Map.get("Index")
              |> Enum.map(fn index_item -> ExAws.Dynamo.Decoder.decode(index_item) end)
              |> Enum.map(fn index_item -> %{index_item | "context" => String.to_atom(index_item["context"])} end)
              |> Enum.reduce(%{}, fn (index, map) -> Map.put(map, {index["context"], index["entity_id"]},  index["events"]) end)
              #unprocessed keys
  end

  defp do_write_events(events, event_put_request) do
    case ExAws.Dynamo.batch_write_item(%{@event_table => event_put_request}) |> ExAws.request do
      {:error, reason} ->
        IO.puts "Error writing events to dynamo, reason: #{inspect reason}"
        IO.inspect events
      _ ->
        :ok
    end
  end


  defp write_index(events) do
    events = events
             |> Enum.map(fn event -> %Perhap.Event{event | event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order,
                                                           metadata: Map.from_struct(event.metadata)}
                                     |> Map.from_struct end)

    index_keys = make_index_keys(events)

    index = retrieve_index(index_keys)

    index_put_request = process_index(events, index)

    case ExAws.Dynamo.batch_write_item(%{@index_table => index_put_request}) |> ExAws.request do
      {:error, reason} ->
        IO.puts "Error writing index to dynamo, reason: #{inspect reason}"
        IO.inspect {:events, events}
        IO.inspect {:index, index_put_request}

        Enum.each(events, fn event ->
          ExAws.Dynamo.delete_item(@event_table, %{event_id: event.event_id |> Perhap.Event.uuid_v1_to_time_order})
          |> ExAws.request! end)
      _ ->
        :ok
    end
  end

end
