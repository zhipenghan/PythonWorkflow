import subprocess
import yaml
import sys
import os
from pathlib import Path

def run_pipeline(yaml_file):
    with open(yaml_file) as f:
        config = yaml.safe_load(f)

    # Handle both old and new YAML formats
    if 'pipeline' in config:
        # Old format
        steps = config['pipeline']
    elif 'steps' in config:
        # New format
        steps = config['steps']
    else:
        print("❌ Invalid pipeline configuration. Expected 'pipeline' or 'steps' key.")
        return

    total_steps = len(steps)
    step_number = 0
    
    # Print pipeline header
    pipeline_name = config.get('name', 'Unnamed Pipeline')
    pipeline_description = config.get('description', 'No description')
    print(f"\n🔧 Pipeline: {pipeline_name}")
    print(f"📝 Description: {pipeline_description}")
    print(f"📊 Total steps: {total_steps}")

    for step in steps:
        step_number += 1
        name = step.get('name', step.get('id', f'Step {step_number}'))
        
        # Handle both old and new formats
        if 'script' in step:
            script = Path(step['script'])
            params = step.get('params', {})
        elif 'component' in step:
            script = Path(step['component'])
            params = step.get('parameters', {})
        else:
            print(f"❌ Invalid step configuration for step {step_number}")
            break

        args = []
        for key, value in params.items():
            # Handle different parameter formats
            if key.startswith('--'):
                flag = key  # Already has --
            else:
                flag = f"--{key.replace('_', '-')}"
            
            if isinstance(value, bool):
                if value:
                    args.append(flag)
            elif isinstance(value, list):
                args.append(flag)
                for item in value:
                    args.append(str(item))
            else:
                args.append(flag)
                args.append(str(value))

        # Create full command for display
        cmd_str = f"python {script} {' '.join(args)}"
        
        # Print step information
        print(f"\n{'='*80}")
        print(f"🚀 Running step {step_number}/{total_steps}: {name}")
        print(f"{'='*80}")
        print(f"Script: {script}")
        print(f"Working directory: {os.getcwd()}")
        print(f"Full command:\n{cmd_str}")
        print(f"{'-'*80}")
        
        # Run the command
        result = subprocess.run(['python', str(script)] + args, capture_output=False)

        if result.returncode != 0:
            print(f"\n❌ Step {step_number}/{total_steps} failed: {name}")
            print(f"Return code: {result.returncode}")
            break
        else:
            print(f"\n✅ Step {step_number}/{total_steps} completed successfully: {name}")

    if step_number == total_steps and result.returncode == 0:
        print(f"\n🎉 Pipeline completed successfully! All {total_steps} steps executed.")
    else:
        print(f"\n⚠️ Pipeline stopped at step {step_number}/{total_steps}.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        yaml_file = sys.argv[1]
        print(f"📋 Running pipeline from: {yaml_file}")
        run_pipeline(yaml_file)
    else:
        # Default to sample pipeline
        default_pipeline = "pipelines/simple_pipeline.yaml"
        print(f"📋 No pipeline specified, running default: {default_pipeline}")
        print("💡 Usage: python run_pipeline.py <pipeline.yaml>")
        print("📁 Available pipelines:")
        
        pipeline_dir = Path("pipelines")
        if pipeline_dir.exists():
            for pipeline_file in pipeline_dir.glob("*.yaml"):
                print(f"   - {pipeline_file}")
        
        print(f"\n🚀 Running default pipeline: {default_pipeline}")
        if Path(default_pipeline).exists():
            run_pipeline(default_pipeline)
        else:
            print(f"❌ Default pipeline not found: {default_pipeline}")
            print("Please specify a pipeline file as an argument.")