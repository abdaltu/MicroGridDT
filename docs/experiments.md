# Learning experiments

Each experiment follows the same cycle: state an expectation, change one parameter
of the live (or simulated) system, observe dashboards and orchestrator logs, and
explain the result.

| # | Experiment | What is varied and observed | Simulator support |
|---|---|---|---|
| 1 | Model, shadow, or twin? | Classify the running system; reclassify once the contactor return path is used (experiment 6) | any scenario |
| 2 | Edge buffering under outage | Polling interval; cut the network link; count buffered, delayed, lost readings | `--interval`, `fault` |
| 3 | Decoupling and cadence | Add/remove Kafka consumers; change DAG cadence; latency and backlog | `--speed` |
| 4 | Storage trade-offs | Raw readings vs rolled-up aggregates; storage growth and query time | `--steps 1440 --speed 0` |
| 5 | Twinning rate and fidelity | Sampling and dashboard refresh rates; data freshness | `--interval` |
| 6 | Closing the loop | Rule that switches the supply contactor when battery voltage is low; force the condition | `loadstep` drives battery down |

Safety: switching AC sources on the physical testbed must be supervised by
qualified personnel; unsupervised work should use the simulator.
