import time
from ortools.sat.python import cp_model

# Enhanced solution callback with more details
class SolutionCallback(cp_model.CpSolverSolutionCallback):
            def __init__(self, logger, shift_vars, workers, days_of_year, min_time_before_gap = 120, stagnation_time = 30, min_gap_improvement = 0.5):
                cp_model.CpSolverSolutionCallback.__init__(self)
                self.logger = logger
                self.solution_count = 0
                self.start_time = time.time()
                self.shift_vars = shift_vars
                self.workers = workers
                self.days_of_year = days_of_year
                self.best_objective = float('inf')

                self.min_time_before_gap = min_time_before_gap
                self.stagnation_time = stagnation_time
                self.min_gap_improvement = min_gap_improvement
                self.best_gap = float("inf")
                self.last_gap_improvement = self.start_time

            def on_solution_callback(self):
                current_time = time.time()
                elapsed_time = current_time - self.start_time
                self.solution_count += 1
                current_objective = self.ObjectiveValue()
                best_bound = self.BestObjectiveBound()
                
                # Calculate the gap
                if current_objective != 0:
                    gap_percent = ((current_objective - best_bound) / abs(current_objective)) * 100
                else:
                    gap_percent = 0.0
                
                # Check if this is a better solution
                is_better = current_objective < self.best_objective
                if is_better:
                    self.best_objective = current_objective
                
                self.logger.info(f"Solution #{self.solution_count} found! Time: {elapsed_time:.2f}s")
                self.logger.info(f"  - Absolute and Relative Gap: {current_objective - best_bound}, {gap_percent:.2f}%")
                self.logger.info(f"  - Branches and Conflicts: {self.NumBranches()}, {self.NumConflicts()}")
                
                # Optional: Log some solution details
                if self.solution_count <= 3:  # Only for first few solutions to avoid spam
                    assigned_shifts = 0
                    for key, var in self.shift_vars.items():
                        if self.Value(var) == 1:
                            assigned_shifts += 1
                    
                    self.logger.info(f"  - Total assigned shifts: {assigned_shifts}")

                if elapsed_time > self.min_time_before_gap:

                    if self.best_gap - gap_percent >= self.min_gap_improvement:
                        self.best_gap = gap_percent
                        self.last_gap_improvement = current_time
            
                        self.logger.info(
                            f"Gap improved to {gap_percent:.2f}%"
                        )
            
                    stagnant = current_time - self.last_gap_improvement
            
                    if stagnant >= self.stagnation_time:
                        self.logger.info(
                            f"No significant gap improvement "
                            f"({self.min_gap_improvement:.2f}%) "
                            f"for {stagnant:.1f}s. Stopping."
                        )
                        self.StopSearch()