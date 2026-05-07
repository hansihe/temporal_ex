defmodule Temporalex.TemporalCliSmokeTest do
  use ExUnit.Case, async: false

  @moduletag :external

  test "Temporal CLI exposes the local development server command" do
    temporal =
      System.find_executable("temporal") || flunk("temporal CLI executable was not found")

    assert {help, 0} =
             System.cmd(temporal, ["server", "start-dev", "--help"], stderr_to_stdout: true)

    assert help =~ "temporal server start-dev"
    assert help =~ "--headless"
  end
end
