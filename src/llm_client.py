"""
LLM Client voor lokale inference via LM Studio of llama.cpp server.

Communiceert met het fine-tuned Llama 3.2 3B model voor trading signal generatie.
"""

import logging
import json
import re
from typing import Dict, Optional, Any
import httpx
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class LLMClient:
    """
    Client voor communicatie met lokale LLM service.
    
    Ondersteunt:
    - LM Studio (http://localhost:1234/v1)
    - llama.cpp server (http://localhost:8080/v1)
    - Ollama (http://localhost:11434/v1)
    
    Alle gebruiken OpenAI-compatible API.
    """
    
    # System prompt voor het model - verbeterd JSON schema
    SYSTEM_PROMPT = """Je bent een crypto trading analist gespecialiseerd in het analyseren van Truth Social posts van Donald Trump.
Analyseer de post en bepaal het handelssignaal voor TRUMP coin.

Geef je antwoord ALLEEN in strict JSON format:
{
  "signal": "BUY | SELL | HOLD",
  "impact_level": "NO_IMPACT | WEAK | MODERATE | STRONG",
  "confidence": 0.0-1.0,
  "horizon": "SHORT | MEDIUM | LONG",
  "expected_move_pct": -10.0 to +10.0,
  "reasoning": "korte uitleg"
}

Horizon uitleg:
- SHORT: impact binnen 1 uur
- MEDIUM: impact binnen 1-4 uur  
- LONG: impact pas na 4+ uur

Voorbeeld output:
{"signal": "BUY", "impact_level": "MODERATE", "confidence": 0.75, "horizon": "SHORT", "expected_move_pct": 3.5, "reasoning": "Positieve uitspraak over crypto"}"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize LLM client.
        
        Args:
            config: LLM configuratie dict met base_url, model, timeout, etc.
        """
        self.base_url = config.get('base_url', 'http://localhost:1234/v1')
        self.model = config.get('model', 'trump_signal_llama3b')
        self.timeout = config.get('timeout', 60)
        self.max_retries = config.get('max_retries', 3)
        self.temperature = config.get('temperature', 0.1)
        self.max_tokens = config.get('max_tokens', 256)
        
        # HTTP client
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"Content-Type": "application/json"}
        )
        
        logger.info(f"LLM Client initialized: {self.base_url}")
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def health_check(self) -> bool:
        """
        Check of LLM service beschikbaar is.
        
        Returns:
            True als service bereikbaar is
        """
        try:
            # Probeer models endpoint
            response = self.client.get("/models")
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"LLM health check failed: {e}")
            return False
    
    def _parse_json_response(self, content: str) -> Optional[Dict[str, Any]]:
        """
        Parse JSON uit LLM response, ook als er extra tekst omheen staat.
        
        Args:
            content: Raw LLM response
            
        Returns:
            Parsed JSON dict of None
        """
        # Verwijder eventuele markdown code blocks
        if '```json' in content:
            start = content.find('```json') + 7
            end = content.find('```', start)
            content = content[start:end].strip()
        elif '```' in content:
            start = content.find('```') + 3
            end = content.find('```', start)
            content = content[start:end].strip()
        
        # Probeer JSON te vinden in de tekst
        json_match = re.search(r'\{[^{}]*\}', content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        
        # Fallback: probeer hele content als JSON
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            logger.warning(f"Could not parse JSON from response: {content[:200]}")
            return None
    
    def _normalize_signal(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normaliseer en valideer het geparsde signaal met verbeterd schema.
        
        Args:
            parsed: Geparsde JSON response
            
        Returns:
            Genormaliseerd signaal dict met signal, impact_level, confidence, horizon, expected_move_pct, reasoning
        """
        # Signal validatie
        signal = str(parsed.get('signal', 'HOLD')).upper()
        if signal not in ['BUY', 'SELL', 'HOLD']:
            signal = 'HOLD'
        
        # Impact level validatie
        impact_level = str(parsed.get('impact_level', 'NO_IMPACT')).upper()
        if impact_level not in ['NO_IMPACT', 'WEAK', 'MODERATE', 'STRONG']:
            impact_level = 'NO_IMPACT'
        
        # Confidence validatie
        try:
            confidence = float(parsed.get('confidence', 0.5))
            confidence = max(0.0, min(1.0, confidence))
        except (TypeError, ValueError):
            confidence = 0.5
        
        # Horizon validatie
        horizon = str(parsed.get('horizon', 'SHORT')).upper()
        if horizon not in ['SHORT', 'MEDIUM', 'LONG']:
            horizon = 'SHORT'
        
        # Expected move validatie
        try:
            expected_move_pct = float(parsed.get('expected_move_pct', 0.0))
            expected_move_pct = max(-20.0, min(20.0, expected_move_pct))  # Clamp to reasonable range
        except (TypeError, ValueError):
            expected_move_pct = 0.0
        
        # Reasoning
        reasoning = str(parsed.get('reasoning', ''))[:500]
        
        return {
            'signal': signal,
            'impact_level': impact_level,
            'confidence': confidence,
            'horizon': horizon,
            'expected_move_pct': expected_move_pct,
            'reasoning': reasoning
        }
    
    def analyze_post(self, post_content: str) -> Dict[str, Any]:
        """
        Analyseer een post en genereer trading signaal met verbeterd schema.
        
        Args:
            post_content: Plain text content van de post
            
        Returns:
            Dict met signal, impact_level, confidence, horizon, expected_move_pct, reasoning, success, error
        """
        result = {
            'signal': 'HOLD',
            'impact_level': 'NO_IMPACT',
            'confidence': 0.0,
            'horizon': 'SHORT',
            'expected_move_pct': 0.0,
            'reasoning': '',
            'success': False,
            'error': None,
            'raw_response': None,
            'analyzed_at': datetime.now(timezone.utc).isoformat()
        }
        
        if not post_content or not post_content.strip():
            result['error'] = 'Empty post content'
            return result
        
        # Bouw request
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": f"Analyseer deze post:\n\n{post_content}"}
        ]
        
        request_body = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens
        }
        
        # Retry loop
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"LLM request attempt {attempt + 1}/{self.max_retries}")
                
                response = self.client.post(
                    "/chat/completions",
                    json=request_body
                )
                response.raise_for_status()
                
                data = response.json()
                result['raw_response'] = data
                
                # Extract content
                if 'choices' in data and len(data['choices']) > 0:
                    content = data['choices'][0].get('message', {}).get('content', '')
                    
                    # Parse JSON
                    parsed = self._parse_json_response(content)
                    
                    if parsed:
                        normalized = self._normalize_signal(parsed)
                        result.update(normalized)
                        result['success'] = True
                        
                        logger.info(f"Signal generated: {result['signal']} "
                                  f"(confidence: {result['confidence']:.2f})")
                        return result
                    else:
                        # Fallback: probeer sentiment uit tekst te halen
                        content_upper = content.upper()
                        if 'BUY' in content_upper:
                            result['signal'] = 'BUY'
                        elif 'SELL' in content_upper:
                            result['signal'] = 'SELL'
                        result['confidence'] = 0.3
                        result['reasoning'] = content[:200]
                        result['success'] = True
                        result['error'] = 'Fallback parsing used'
                        return result
                else:
                    result['error'] = 'Invalid response format'
                    
            except httpx.HTTPStatusError as e:
                result['error'] = f"HTTP error {e.response.status_code}"
                logger.warning(f"LLM request failed: {result['error']}")
                
            except httpx.TimeoutException:
                result['error'] = 'Request timeout'
                logger.warning("LLM request timed out")
                
            except Exception as e:
                result['error'] = str(e)
                logger.error(f"LLM request error: {e}")
            
            # Retry delay
            if attempt < self.max_retries - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error(f"All LLM attempts failed: {result['error']}")
        return result


class AsyncLLMClient:
    """
    Async versie van LLM Client voor concurrent processing.
    """
    
    SYSTEM_PROMPT = LLMClient.SYSTEM_PROMPT
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize async client."""
        self.base_url = config.get('base_url', 'http://localhost:1234/v1')
        self.model = config.get('model', 'trump_signal_llama3b')
        self.timeout = config.get('timeout', 60)
        self.max_retries = config.get('max_retries', 3)
        self.temperature = config.get('temperature', 0.1)
        self.max_tokens = config.get('max_tokens', 256)
        
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"Content-Type": "application/json"}
        )
    
    async def close(self):
        """Close async client."""
        await self.client.aclose()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def analyze_post(self, post_content: str) -> Dict[str, Any]:
        """
        Async versie van analyze_post met verbeterd schema.
        
        Args:
            post_content: Plain text content
            
        Returns:
            Signal dict met alle velden
        """
        # Implementatie vrijwel identiek aan sync versie
        # maar met async/await
        result = {
            'signal': 'HOLD',
            'impact_level': 'NO_IMPACT',
            'confidence': 0.0,
            'horizon': 'SHORT',
            'expected_move_pct': 0.0,
            'reasoning': '',
            'success': False,
            'error': None,
            'analyzed_at': datetime.now(timezone.utc).isoformat()
        }
        
        if not post_content or not post_content.strip():
            result['error'] = 'Empty post content'
            return result
        
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": f"Analyseer deze post:\n\n{post_content}"}
        ]
        
        request_body = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens
        }
        
        for attempt in range(self.max_retries):
            try:
                response = await self.client.post(
                    "/chat/completions",
                    json=request_body
                )
                response.raise_for_status()
                
                data = response.json()
                
                if 'choices' in data and len(data['choices']) > 0:
                    content = data['choices'][0].get('message', {}).get('content', '')
                    
                    # Parse JSON (use sync helper)
                    parsed = LLMClient._parse_json_response(None, content)
                    
                    if parsed:
                        normalized = LLMClient._normalize_signal(None, parsed)
                        result.update(normalized)
                        result['success'] = True
                        return result
                        
            except Exception as e:
                result['error'] = str(e)
                
                if attempt < self.max_retries - 1:
                    import asyncio
                    await asyncio.sleep(2 ** attempt)
        
        return result

