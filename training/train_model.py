"""
# INSTRUCTIONS:
# Dit script traint het Llama 3.2 3B model met QLoRA via Unsloth.
# MOET IN WSL2 DRAAIEN vanwege bitsandbytes compatibiliteit!
#
# Setup in WSL2:
#   1. pip install "unsloth[colab-new] @ git+https://github.com/unslothai/unsloth.git"
#   2. pip install --no-deps trl peft accelerate bitsandbytes
#   3. Kopieer training data naar WSL2 filesystem
#
# Draai in WSL2:
#   cd /mnt/f/Containers/truthpuller_v2
#   python training/train_model.py
#
# Output: models/trump_signal_llama3b/ (LoRA adapter + GGUF export)
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Optional
import torch

# Check of we in WSL2 draaien
def check_wsl():
    """Check if running in WSL2."""
    try:
        with open('/proc/version', 'r') as f:
            version = f.read().lower()
            if 'microsoft' in version or 'wsl' in version:
                return True
    except:
        pass
    return False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_dataset(data_path: Path):
    """
    Laad training dataset in Alpaca format.
    
    Args:
        data_path: Path naar JSONL file
        
    Returns:
        HuggingFace Dataset
    """
    from datasets import load_dataset
    
    dataset = load_dataset('json', data_files=str(data_path), split='train')
    logger.info(f"Loaded {len(dataset)} training examples")
    return dataset


def format_prompt(example):
    """
    Format een training voorbeeld voor het model.
    
    Args:
        example: Dict met instruction, input, output
        
    Returns:
        Geformatteerde prompt string
    """
    # Alpaca-style format
    text = f"""### Instruction:
{example['instruction']}

### Input:
{example['input']}

### Response:
{example['output']}"""
    
    return {"text": text}


def train_model(
    data_path: Path,
    output_dir: Path,
    model_name: str = "unsloth/Llama-3.2-3B-Instruct",
    max_seq_length: int = 2048,
    lora_r: int = 16,
    lora_alpha: int = 16,
    batch_size: int = 2,
    gradient_accumulation: int = 4,
    max_steps: int = 100,
    learning_rate: float = 2e-4,
    save_gguf: bool = True
):
    """
    Train het model met QLoRA via Unsloth.
    
    Args:
        data_path: Path naar training JSONL
        output_dir: Output directory voor model
        model_name: HuggingFace model ID
        max_seq_length: Maximum sequence length
        lora_r: LoRA rank
        lora_alpha: LoRA alpha
        batch_size: Training batch size
        gradient_accumulation: Gradient accumulation steps
        max_steps: Maximum training steps
        learning_rate: Learning rate
        save_gguf: Of GGUF export moet worden gemaakt
    """
    from unsloth import FastLanguageModel
    from trl import SFTTrainer, SFTConfig
    from datasets import load_dataset
    
    logger.info(f"Loading model: {model_name}")
    logger.info(f"CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        logger.info(f"GPU: {torch.cuda.get_device_name(0)}")
        logger.info(f"VRAM: {torch.cuda.get_device_properties(0).total_memory / 1e9:.1f} GB")
    
    # Load model met 4-bit quantization
    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name=model_name,
        max_seq_length=max_seq_length,
        dtype=None,  # Auto-detect
        load_in_4bit=True,  # Essentieel voor 16GB VRAM
    )
    
    # Configureer LoRA adapters
    model = FastLanguageModel.get_peft_model(
        model,
        r=lora_r,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj",
                       "gate_proj", "up_proj", "down_proj"],
        lora_alpha=lora_alpha,
        lora_dropout=0,  # Geoptimaliseerd voor snelheid
        bias="none",
        use_gradient_checkpointing="unsloth",  # Bespaart VRAM
        random_state=42,
        max_seq_length=max_seq_length,
    )
    
    # Log trainbare parameters
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    total_params = sum(p.numel() for p in model.parameters())
    logger.info(f"Trainable parameters: {trainable_params:,} ({100*trainable_params/total_params:.2f}%)")
    
    # Laad dataset
    dataset = load_dataset('json', data_files=str(data_path), split='train')
    logger.info(f"Training on {len(dataset)} examples")
    
    # Format dataset
    dataset = dataset.map(format_prompt, remove_columns=dataset.column_names)
    
    # Configureer trainer
    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=dataset,
        dataset_text_field="text",
        max_seq_length=max_seq_length,
        args=SFTConfig(
            per_device_train_batch_size=batch_size,
            gradient_accumulation_steps=gradient_accumulation,
            warmup_steps=10,
            max_steps=max_steps,
            learning_rate=learning_rate,
            fp16=not torch.cuda.is_bf16_supported(),
            bf16=torch.cuda.is_bf16_supported(),
            logging_steps=5,
            optim="adamw_8bit",  # Bespaart geheugen
            weight_decay=0.01,
            lr_scheduler_type="linear",
            seed=42,
            output_dir=str(output_dir),
            report_to="none",  # Geen W&B logging
        ),
    )
    
    # Start training
    logger.info("Starting training...")
    logger.info(f"Effective batch size: {batch_size * gradient_accumulation}")
    
    trainer.train()
    
    logger.info("Training complete!")
    
    # Sla LoRA adapter op
    lora_path = output_dir / "lora_adapter"
    model.save_pretrained(str(lora_path))
    tokenizer.save_pretrained(str(lora_path))
    logger.info(f"Saved LoRA adapter to {lora_path}")
    
    # Export naar GGUF voor llama.cpp / LM Studio
    if save_gguf:
        logger.info("Exporting to GGUF format...")
        try:
            gguf_path = output_dir / "trump_signal_llama3b"
            model.save_pretrained_gguf(
                str(gguf_path),
                tokenizer,
                quantization_method="q4_k_m"  # Goede balans kwaliteit/grootte
            )
            logger.info(f"Saved GGUF model to {gguf_path}")
        except Exception as e:
            logger.error(f"GGUF export failed: {e}")
            logger.info("You can manually convert using llama.cpp's convert script")
    
    return model, tokenizer


def main():
    """Main entry point."""
    # Check WSL2
    if not check_wsl():
        logger.warning("=" * 60)
        logger.warning("WARNING: This script should run in WSL2!")
        logger.warning("Windows heeft problemen met bitsandbytes.")
        logger.warning("=" * 60)
        
        response = input("Wil je toch doorgaan? (y/N): ")
        if response.lower() != 'y':
            logger.info("Afgebroken. Start dit script in WSL2.")
            sys.exit(0)
    
    # Paths
    project_root = Path(__file__).parent.parent
    data_path = project_root / "data" / "training_dataset.jsonl"
    output_dir = project_root / "models"
    
    # Check of dataset bestaat
    if not data_path.exists():
        logger.error(f"Training dataset not found: {data_path}")
        logger.error("Run build_training_dataset.py first!")
        sys.exit(1)
    
    # Maak output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Train model
    model, tokenizer = train_model(
        data_path=data_path,
        output_dir=output_dir,
        model_name="unsloth/Llama-3.2-3B-Instruct",
        max_seq_length=2048,
        lora_r=16,
        lora_alpha=16,
        batch_size=2,
        gradient_accumulation=4,
        max_steps=100,  # Pas aan op basis van dataset grootte
        learning_rate=2e-4,
        save_gguf=True
    )
    
    logger.info("=" * 60)
    logger.info("Training voltooid!")
    logger.info(f"Model opgeslagen in: {output_dir}")
    logger.info("=" * 60)
    logger.info("\nVolgende stappen:")
    logger.info("1. Kopieer het GGUF model naar Windows")
    logger.info("2. Laad het model in LM Studio")
    logger.info("3. Start de signal generator")


if __name__ == "__main__":
    main()

