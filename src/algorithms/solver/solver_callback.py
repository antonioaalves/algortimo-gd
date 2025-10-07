import time
from ortools.sat.python import cp_model

# Enhanced solution callback with more details
class SolutionCallback(cp_model.CpSolverSolutionCallback):
            def __init__(self, logger, shift_vars, workers, days_of_year):
                cp_model.CpSolverSolutionCallback.__init__(self)
                self.logger = logger
                self.solution_count = 0
                self.start_time = time.time()
                self.shift_vars = shift_vars
                self.workers = workers
                self.days_of_year = days_of_year
                self.best_objective = float('inf')

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
                
                self.logger.info(f"Solution #{self.solution_count} found! {'[BETTER]' if is_better else ''}")
                self.logger.info(f"  - Time: {elapsed_time:.2f}s")
                self.logger.info(f"  - Current objective: {current_objective} {'(NEW BEST!)' if is_better else ''}")
                self.logger.info(f"  - Lower bound: {best_bound}")
                self.logger.info(f"  - Gap: {gap_percent:.2f}%")
                self.logger.info(f"  - Branches: {self.NumBranches()}")
                self.logger.info(f"  - Conflicts: {self.NumConflicts()}")
                
                # Optional: Log some solution details
                if self.solution_count <= 3:  # Only for first few solutions to avoid spam
                    assigned_shifts = 0
                    for key, var in self.shift_vars.items():
                        if self.Value(var) == 1:
                            assigned_shifts += 1
                    
                    self.logger.info(f"  - Total assigned shifts: {assigned_shifts}")
