from base_data_project.log_config import get_logger
from src.config import PROJECT_NAME

logger = get_logger(PROJECT_NAME)

def analyze_optimization_results(solver, optimization_details):
    """
    Analyze the optimization results after solving.
    """
    results = {}
    
    # Point 1: Pessimistic objective deviations
    point_1_results = {
        'deviations': {},
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_1_pessobj_deviations']['penalty_weight']
    
    for (d, s), var_info in optimization_details['point_1_pessobj_deviations']['variables'].items():
        # NOW we can get the actual values
        pos_diff_value = solver.Value(var_info['pos_diff'])
        neg_diff_value = solver.Value(var_info['neg_diff'])
        target = var_info['target']
        
        # Calculate the actual difference
        if pos_diff_value > 0:
            actual_diff = pos_diff_value  # Excess workers
        elif neg_diff_value > 0:
            actual_diff = -neg_diff_value  # Shortage (negative)
        else:
            actual_diff = 0  # Perfect match
        
        penalty = (pos_diff_value + neg_diff_value) * penalty_weight
        
        point_1_results['deviations'][(d, s)] = {
            'target_workers': target,
            'actual_diff': actual_diff,
            'penalty': penalty
        }
        
        point_1_results['total_penalty'] += penalty
    
    results['point_1_pessobj_deviations'] = point_1_results
    
    # Point 2: Consecutive free days bonus
    point_2_results = {
        'consecutive_pairs': [],
        'total_bonus': 0
    }
    
    bonus_weight = optimization_details['point_2_consecutive_free_days']['weight']
    
    for var_info in optimization_details['point_2_consecutive_free_days']['variables']:
        if solver.Value(var_info['variable']) == 1:
            point_2_results['consecutive_pairs'].append({
                'worker': var_info['worker'],
                'day1': var_info['day1'],
                'day2': var_info['day2']
            })
            point_2_results['total_bonus'] += abs(bonus_weight)
    
    results['point_2_consecutive_free_days'] = point_2_results
    
    # Point 3: No workers penalty
    point_3_results = {
        'no_worker_days': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_3_no_workers']['penalty_weight']
    
    for (d, s), var_info in optimization_details['point_3_no_workers']['variables'].items():
        if solver.Value(var_info['variable']) == 1:
            penalty = penalty_weight
            point_3_results['no_worker_days'].append({
                'day': d,
                'shift': s,
                'target': var_info['target'],
                'penalty': penalty
            })
            point_3_results['total_penalty'] += penalty
    
    results['point_3_no_workers'] = point_3_results
    
    # Point 4: Minimum workers penalty
    point_4_results = {
        'shortfalls': {},
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_4_min_workers']['penalty_weight']
    
    for (d, s), var_info in optimization_details['point_4_min_workers']['variables'].items():
        shortfall_value = solver.Value(var_info['shortfall'])
        if shortfall_value > 0:
            penalty = shortfall_value * penalty_weight
            point_4_results['shortfalls'][(d, s)] = {
                'min_required': var_info['min_required'],
                'shortfall': shortfall_value,
                'penalty': penalty
            }
            point_4_results['total_penalty'] += penalty
    
    results['point_4_min_workers'] = point_4_results
    
    # Point 5.1: Sunday balance penalty
    point_5_1_results = {
        'worker_segments': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_5_1_sunday_balance']['penalty_weight']
    
    for var_info in optimization_details['point_5_1_sunday_balance']['variables']:
        over_penalty_value = solver.Value(var_info['over_penalty'])
        under_penalty_value = solver.Value(var_info['under_penalty'])
        
        if over_penalty_value > 0 or under_penalty_value > 0:
            penalty = (over_penalty_value + under_penalty_value) * penalty_weight
            point_5_1_results['worker_segments'].append({
                'worker': var_info['worker'],
                'segment': var_info['segment'],
                'ideal_count': var_info['ideal_count'],
                'over_penalty': over_penalty_value,
                'under_penalty': under_penalty_value,
                'penalty': penalty,
                'segment_sundays': var_info['segment_sundays']
            })


            # logger.info(f"Point 5.1 - Worker {var_info['worker']}, Segment {var_info['segment']}, "
            #             f"Ideal Count {var_info['ideal_count']}, Over Penalty {over_penalty_value}, "
            #             f"Under Penalty {under_penalty_value}, Segment Sundays {var_info['segment_sundays']}, ")
            point_5_1_results['total_penalty'] += penalty
    
    results['point_5_1_sunday_balance'] = point_5_1_results
    
    # Point 5.2: C2D balance penalty
    point_5_2_results = {
        'worker_segments': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_5_2_c2d_balance']['penalty_weight']
    
    for var_info in optimization_details['point_5_2_c2d_balance']['variables']:
        over_penalty_value = solver.Value(var_info['over_penalty'])
        under_penalty_value = solver.Value(var_info['under_penalty'])
        
        if over_penalty_value > 0 or under_penalty_value > 0:
            penalty = (over_penalty_value + under_penalty_value) * penalty_weight
            point_5_2_results['worker_segments'].append({
                'worker': var_info['worker'],
                'segment': var_info['segment'],
                'ideal_count': var_info['ideal_count'],
                'over_penalty': over_penalty_value,
                'under_penalty': under_penalty_value,
                'penalty': penalty,
                'segment_weekends': var_info['segment_weekends']
            })

            # logger.info(f"Point 5.2 - Worker {var_info['worker']}, Segment {var_info['segment']}, "
            #             f"Ideal Count {var_info['ideal_count']}, Over Penalty {over_penalty_value}, "
            #             f"Under Penalty {under_penalty_value}, Segment Weekends {var_info['segment_weekends']}, ")
            point_5_2_results['total_penalty'] += penalty
    
    results['point_5_2_c2d_balance'] = point_5_2_results
    
    # Point 6: Inconsistent shifts penalty
    point_6_results = {
        'inconsistent_weeks': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_6_inconsistent_shifts']['penalty_weight']
    
    for (w, week), var_info in optimization_details['point_6_inconsistent_shifts']['variables'].items():
        if solver.Value(var_info['inconsistent_variable']) == 1:
            penalty = penalty_weight
            point_6_results['inconsistent_weeks'].append({
                'worker': w,
                'week': week,
                'has_m_shift': solver.Value(var_info['has_m_shift']) == 1,
                'has_t_shift': solver.Value(var_info['has_t_shift']) == 1,
                'working_days_in_week': var_info['working_days_in_week'],
                'penalty': penalty
            })
            point_6_results['total_penalty'] += penalty
    
    results['point_6_inconsistent_shifts'] = point_6_results
    
    # Point 7: Sunday balance across workers
    point_7_results = {
        'worker_pairs': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_7_sunday_balance_across_workers']['penalty_weight']
    
    for var_info in optimization_details['point_7_sunday_balance_across_workers']['variables']:
        pos_diff_value = solver.Value(var_info['proportional_diff_pos'])
        neg_diff_value = solver.Value(var_info['proportional_diff_neg'])
        
        if pos_diff_value > 0 or neg_diff_value > 0:
            penalty = (pos_diff_value + neg_diff_value) * penalty_weight // 2
            
            w1_total = solver.Value(var_info['total_sunday_free_w1'])
            w2_total = solver.Value(var_info['total_sunday_free_w2'])
            
            point_7_results['worker_pairs'].append({
                'worker1': var_info['worker1'],
                'worker2': var_info['worker2'],
                'worker1_sunday_free': w1_total,
                'worker2_sunday_free': w2_total,
                'prop1': var_info['prop1'],
                'prop2': var_info['prop2'],
                'proportional_diff_pos': pos_diff_value,
                'proportional_diff_neg': neg_diff_value,
                'penalty': penalty
            })
            logger.info(f"Point 7 - Worker1 {var_info['worker1']}, Worker2 {var_info['worker2']}, "
                        f"Worker1 Sunday Free {w1_total}, Worker2 Sunday Free {w2_total}, "
                        f"Prop1 {var_info['prop1']}, Prop2 {var_info['prop2']}, "
                        f"Proportional Diff Pos {pos_diff_value}, Proportional Diff Neg {neg_diff_value}, Penalty {penalty}")
            point_7_results['total_penalty'] += penalty
    
    results['point_7_sunday_balance_across_workers'] = point_7_results
    
    # Point 7B: LQ balance across workers
    point_7b_results = {
        'worker_pairs': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_7b_lq_balance_across_workers']['penalty_weight']
    
    for var_info in optimization_details['point_7b_lq_balance_across_workers']['variables']:
        pos_diff_value = solver.Value(var_info['diff_pos'])
        neg_diff_value = solver.Value(var_info['diff_neg'])
        
        if pos_diff_value > 0 or neg_diff_value > 0:
            penalty = (pos_diff_value + neg_diff_value) * penalty_weight // 2
            
            w1_total = solver.Value(var_info['total_lq_free_w1'])
            w2_total = solver.Value(var_info['total_lq_free_w2'])
            
            point_7b_results['worker_pairs'].append({
                'worker1': var_info['worker1'],
                'worker2': var_info['worker2'],
                'worker1_lq_free': w1_total,
                'worker2_lq_free': w2_total,
                'prop1': var_info['prop1'],
                'prop2': var_info['prop2'],
                'diff_pos': pos_diff_value,
                'diff_neg': neg_diff_value,
                'penalty': penalty
            })

            logger.info(f"Point 7B - Worker1 {var_info['worker1']}, Worker2 {var_info['worker2']}, "
                        f"Worker1 LQ Free {w1_total}, Worker2 LQ Free {w2_total}, "
                        f"Prop1 {var_info['prop1']}, Prop2 {var_info['prop2']}, "
                        f"Diff Pos {pos_diff_value}, Diff Neg {neg_diff_value}, Penalty {penalty}")
            point_7b_results['total_penalty'] += penalty
    
    results['point_7b_lq_balance_across_workers'] = point_7b_results
    
    # Point 8: Manager/Keyholder conflicts
    point_8_results = {
        'conflicts': {},
        'total_penalty': 0
    }
    
    penalty_weights = optimization_details['point_8_manager_keyholder_conflicts']['penalty_weights']
    
    for (d, conflict_type), var_info in optimization_details['point_8_manager_keyholder_conflicts']['variables'].items():
        if conflict_type == 'mgr_kh_same_off':
            if solver.Value(var_info['both_off']) == 1:
                penalty = penalty_weights['mgr_kh_same_off']
                point_8_results['conflicts'][(d, conflict_type)] = {
                    'day': d,
                    'type': 'Manager and Keyholder both off',
                    'mgr_any': solver.Value(var_info['mgr_any']) == 1,
                    'kh_any': solver.Value(var_info['kh_any']) == 1,
                    'penalty': penalty
                }
                point_8_results['total_penalty'] += penalty
                
        elif conflict_type == 'kh_overlap':
            if solver.Value(var_info['kh_overlap']) == 1:
                penalty = penalty_weights['kh_overlap']
                point_8_results['conflicts'][(d, conflict_type)] = {
                    'day': d,
                    'type': 'Multiple Keyholders off',
                    'penalty': penalty
                }
                point_8_results['total_penalty'] += penalty
                
        elif conflict_type == 'mgr_overlap':
            if solver.Value(var_info['mgr_overlap']) == 1:
                penalty = penalty_weights['mgr_overlap']
                point_8_results['conflicts'][(d, conflict_type)] = {
                    'day': d,
                    'type': 'Multiple Managers off',
                    'penalty': penalty
                }
                point_8_results['total_penalty'] += penalty
    
    results['point_8_manager_keyholder_conflicts'] = point_8_results
    
    # Calculate total penalty across all optimization points
    total_penalty = sum([
        point_1_results['total_penalty'],
        point_3_results['total_penalty'],
        point_4_results['total_penalty'],
        point_5_1_results['total_penalty'],
        point_5_2_results['total_penalty'],
        point_6_results['total_penalty'],
        point_7_results['total_penalty'],
        point_7b_results['total_penalty'],
        point_8_results['total_penalty']
    ])
    
    total_bonus = point_2_results['total_bonus']
    
    results['summary'] = {
        'total_penalty': total_penalty,
        'total_bonus': total_bonus,
        'net_objective': total_penalty - total_bonus,
        'point_breakdown': {
            'point_1_pessobj_deviations': point_1_results['total_penalty'],
            'point_2_consecutive_free_days': -point_2_results['total_bonus'],  # negative because it's a bonus
            'point_3_no_workers': point_3_results['total_penalty'],
            'point_4_min_workers': point_4_results['total_penalty'],
            'point_5_1_sunday_balance': point_5_1_results['total_penalty'],
            'point_5_2_c2d_balance': point_5_2_results['total_penalty'],
            'point_6_inconsistent_shifts': point_6_results['total_penalty'],
            'point_7_sunday_balance_across_workers': point_7_results['total_penalty'],
            'point_7b_lq_balance_across_workers': point_7b_results['total_penalty'],
            'point_8_manager_keyholder_conflicts': point_8_results['total_penalty']
        }
    }
    
    return results