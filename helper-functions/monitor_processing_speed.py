#!/usr/bin/env python3
import time
import json
from datetime import datetime
import random

def simulate_processing_task():
    """Simulate a processing task - replace this with your actual processing function"""
    # Replace this with your actual processing logic
    # For now, simulating work with random sleep
    time.sleep(random.uniform(0.1, 0.5))
    return True

def measure_single_operation():
    """Measure the time taken for a single operation"""
    start = time.time()
    result = simulate_processing_task()
    end = time.time()
    return end - start, result

def main():
    print("Starting processing speed measurement...")
    print("Collecting 100 samples...")
    print("-" * 70)
    
    samples = []
    successful_operations = 0
    failed_operations = 0
    
    overall_start = time.time()
    
    for i in range(100):
        sample_start = time.time()
        
        # Measure the actual operation
        operation_time, success = measure_single_operation()
        
        if success:
            successful_operations += 1
        else:
            failed_operations += 1
        
        samples.append({
            'sample_number': i + 1,
            'operation_time': operation_time,
            'success': success,
            'timestamp': datetime.now().isoformat()
        })
        
        # Calculate running statistics
        total_time = sum(s['operation_time'] for s in samples)
        avg_time = total_time / len(samples)
        ops_per_second = 1 / avg_time if avg_time > 0 else 0
        ops_per_minute = ops_per_second * 60
        ops_per_hour = ops_per_minute * 60
        
        # Print progress every 10 samples
        if (i + 1) % 10 == 0:
            print(f"Sample {i+1}/100:")
            print(f"  Last operation: {operation_time:.3f}s")
            print(f"  Average time: {avg_time:.3f}s")
            print(f"  Rate: {ops_per_second:.2f}/sec | {ops_per_minute:.1f}/min | {ops_per_hour:.0f}/hour")
            print(f"  Success rate: {successful_operations}/{i+1} ({successful_operations/(i+1)*100:.1f}%)")
            print("-" * 70)
    
    overall_end = time.time()
    total_elapsed = overall_end - overall_start
    
    # Calculate final statistics
    operation_times = [s['operation_time'] for s in samples]
    min_time = min(operation_times)
    max_time = max(operation_times)
    avg_time = sum(operation_times) / len(operation_times)
    
    # Calculate standard deviation
    variance = sum((t - avg_time) ** 2 for t in operation_times) / len(operation_times)
    std_dev = variance ** 0.5
    
    # Calculate rates
    ops_per_second = 1 / avg_time if avg_time > 0 else 0
    ops_per_minute = ops_per_second * 60
    ops_per_hour = ops_per_minute * 60
    
    print("\n" + "=" * 70)
    print("FINAL ANALYSIS (100 samples):")
    print(f"  Total measurement time: {total_elapsed:.1f} seconds")
    print(f"  Successful operations: {successful_operations}")
    print(f"  Failed operations: {failed_operations}")
    print(f"  Success rate: {successful_operations/100*100:.1f}%")
    print("\nTiming Statistics:")
    print(f"  Min time: {min_time:.3f}s")
    print(f"  Max time: {max_time:.3f}s")
    print(f"  Average time: {avg_time:.3f}s")
    print(f"  Std deviation: {std_dev:.3f}s")
    print("\nProcessing Rates:")
    print(f"  Operations/second: {ops_per_second:.2f}")
    print(f"  Operations/minute: {ops_per_minute:.1f}")
    print(f"  Operations/hour: {ops_per_hour:.0f}")
    
    # Save detailed results
    with open('processing_speed_results.json', 'w') as f:
        json.dump({
            'samples': samples,
            'summary': {
                'total_measurement_time': total_elapsed,
                'successful_operations': successful_operations,
                'failed_operations': failed_operations,
                'success_rate': successful_operations / 100,
                'timing': {
                    'min_seconds': min_time,
                    'max_seconds': max_time,
                    'avg_seconds': avg_time,
                    'std_dev_seconds': std_dev
                },
                'rates': {
                    'per_second': ops_per_second,
                    'per_minute': ops_per_minute,
                    'per_hour': ops_per_hour
                }
            }
        }, f, indent=2)
    print("\nDetailed results saved to processing_speed_results.json")

if __name__ == "__main__":
    main()