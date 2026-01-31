"""
# INSTRUCTIONS:
# Utility module voor het laden van LLM prompts uit TOML bestanden.
# Geen fallback prompts - als bestand ontbreekt wordt een error gegooid.
#
# Gebruik:
#   from prompt_loader import PromptLoader
#   loader = PromptLoader()
#   system_prompt = loader.get_system_prompt()
#   user_prompt = loader.get_phase1_prompt(content="...", timestamp="...", post_type="...")
"""

import sys
from pathlib import Path
from typing import Dict, Optional, Any

# TOML parser - probeer tomllib (Python 3.11+), anders tomli
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        print("ERROR: tomli package required for Python < 3.11")
        print("Install with: pip install tomli")
        sys.exit(1)


class PromptLoader:
    """
    Laadt LLM prompts uit TOML configuratiebestanden.
    
    Prompts worden geladen uit config/prompts/:
    - system.toml: Gedeelde system prompt voor alle analyses
    - phase1_post_analysis.toml: Per-post analyse prompt
    - phase2_cluster_comparison.toml: Cluster vergelijking prompt
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize prompt loader.
        
        Args:
            config_dir: Pad naar config/prompts directory.
                       Indien None, wordt automatisch bepaald.
        """
        if config_dir is None:
            # Bepaal pad relatief aan dit script
            script_dir = Path(__file__).parent
            config_dir = script_dir.parent / 'config' / 'prompts'
        
        self.config_dir = Path(config_dir)
        
        if not self.config_dir.exists():
            raise FileNotFoundError(
                f"Prompts directory niet gevonden: {self.config_dir}\n"
                "Zorg dat de TOML bestanden in config/prompts/ staan."
            )
        
        # Cache voor geladen prompts
        self._cache: Dict[str, Dict] = {}
    
    def _load_toml(self, filename: str) -> Dict:
        """
        Laad een TOML bestand.
        
        Args:
            filename: Bestandsnaam (bijv. "system.toml")
            
        Returns:
            Parsed TOML dict
            
        Raises:
            FileNotFoundError: Als bestand niet bestaat
            ValueError: Als TOML ongeldig is
        """
        # Check cache
        if filename in self._cache:
            return self._cache[filename]
        
        filepath = self.config_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(
                f"Prompt bestand niet gevonden: {filepath}\n"
                "Dit bestand is vereist voor de LLM analyse."
            )
        
        try:
            with open(filepath, 'rb') as f:
                data = tomllib.load(f)
            
            # Cache result
            self._cache[filename] = data
            return data
            
        except Exception as e:
            raise ValueError(f"Fout bij laden van {filepath}: {e}")
    
    def get_system_prompt(self) -> str:
        """
        Haal de system prompt op.
        
        Returns:
            Complete system prompt string (system content + context)
        """
        data = self._load_toml('system.toml')
        
        system_content = data.get('system', {}).get('content', '')
        context_background = data.get('context', {}).get('background', '')
        
        if not system_content:
            raise ValueError("system.toml bevat geen [system].content")
        
        # Combineer system content met context
        if context_background:
            return f"{system_content.strip()}\n\n{context_background.strip()}"
        
        return system_content.strip()
    
    def get_phase1_prompt(
        self,
        content: str,
        timestamp: str,
        post_type: str
    ) -> str:
        """
        Haal Phase 1 user prompt op met placeholders ingevuld.
        
        Args:
            content: Post content tekst
            timestamp: Post timestamp
            post_type: Type post (original, retweet, reply)
            
        Returns:
            Gevulde user prompt string
        """
        data = self._load_toml('phase1_post_analysis.toml')
        
        template = data.get('template', {}).get('user_prompt', '')
        
        if not template:
            raise ValueError("phase1_post_analysis.toml bevat geen [template].user_prompt")
        
        # Vul placeholders in
        return template.format(
            content=content,
            timestamp=timestamp,
            post_type=post_type
        )
    
    def get_phase2_prompt(
        self,
        interval: str,
        return_pct: float,
        sigma: float,
        trigger_time: str,
        post_count: int,
        posts_section: str
    ) -> str:
        """
        Haal Phase 2 user prompt op met placeholders ingevuld.
        
        Args:
            interval: Kline interval (bijv. "15m")
            return_pct: Return percentage
            sigma: Sigma waarde
            trigger_time: Trigger timestamp
            post_count: Aantal posts in cluster
            posts_section: Geformatteerde posts met scores
            
        Returns:
            Gevulde user prompt string
        """
        data = self._load_toml('phase2_cluster_comparison.toml')
        
        template = data.get('template', {}).get('user_prompt', '')
        
        if not template:
            raise ValueError("phase2_cluster_comparison.toml bevat geen [template].user_prompt")
        
        # Vul placeholders in
        # Let op: template bevat {return_pct:+.2f} dus we moeten format anders doen
        return template.format(
            interval=interval,
            return_pct=return_pct,
            sigma=sigma,
            trigger_time=trigger_time,
            post_count=post_count,
            posts_section=posts_section
        )
    
    def get_output_schema(self, phase: int) -> Dict[str, Any]:
        """
        Haal output schema metadata op voor validatie.
        
        Args:
            phase: 1 of 2
            
        Returns:
            Dict met schema informatie
        """
        if phase == 1:
            data = self._load_toml('phase1_post_analysis.toml')
        elif phase == 2:
            data = self._load_toml('phase2_cluster_comparison.toml')
        else:
            raise ValueError(f"Ongeldige phase: {phase}")
        
        return data.get('output_schema', {})
    
    def reload(self):
        """Clear cache en herlaad alle prompts."""
        self._cache.clear()


# Singleton instance voor gemakkelijk gebruik
_loader_instance: Optional[PromptLoader] = None


def get_loader() -> PromptLoader:
    """
    Haal singleton PromptLoader instance op.
    
    Returns:
        PromptLoader instance
    """
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = PromptLoader()
    return _loader_instance


# Convenience functies
def get_system_prompt() -> str:
    """Shortcut voor get_loader().get_system_prompt()"""
    return get_loader().get_system_prompt()


def get_phase1_prompt(content: str, timestamp: str, post_type: str) -> str:
    """Shortcut voor get_loader().get_phase1_prompt()"""
    return get_loader().get_phase1_prompt(content, timestamp, post_type)


def get_phase2_prompt(
    interval: str,
    return_pct: float,
    sigma: float,
    trigger_time: str,
    post_count: int,
    posts_section: str
) -> str:
    """Shortcut voor get_loader().get_phase2_prompt()"""
    return get_loader().get_phase2_prompt(
        interval, return_pct, sigma, trigger_time, post_count, posts_section
    )


if __name__ == "__main__":
    # Test de loader
    print("Testing PromptLoader...")
    
    try:
        loader = PromptLoader()
        
        print("\n=== System Prompt ===")
        system = loader.get_system_prompt()
        print(f"Length: {len(system)} chars")
        print(f"Preview: {system[:200]}...")
        
        print("\n=== Phase 1 Prompt (test) ===")
        phase1 = loader.get_phase1_prompt(
            content="Test post content",
            timestamp="2025-01-17T12:00:00Z",
            post_type="original"
        )
        print(f"Length: {len(phase1)} chars")
        
        print("\n=== Phase 2 Prompt (test) ===")
        phase2 = loader.get_phase2_prompt(
            interval="15m",
            return_pct=5.23,
            sigma=2.5,
            trigger_time="2025-01-17T12:15:00Z",
            post_count=3,
            posts_section="POST 1: Test post..."
        )
        print(f"Length: {len(phase2)} chars")
        
        print("\n[OK] All prompts loaded successfully!")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)

