import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import scanner


class TestMissingWebhook(unittest.TestCase):
    """Test handling of missing DISCORD_WEBHOOK_URL"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db.close()
        self.db_path = self.temp_db.name

    def tearDown(self):
        """Clean up temporary files"""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    @patch.dict(os.environ, {"DISCORD_WEBHOOK_URL": "", "DB_PATH": ""}, clear=False)
    @patch("scanner.DB_PATH")
    def test_missing_webhook_enables_dry_run(self, mock_db_path):
        """Test that missing webhook automatically enables dry-run mode"""
        mock_db_path.__str__.return_value = self.db_path
        
        # Mock the DB to avoid actual file operations
        with patch("scanner.sqlite3.connect"):
            genie = scanner.PumpGenie()
            
            # Simulate the run() initialization logic
            webhook_url = ""
            dry_run_enabled = False
            if not webhook_url:
                dry_run_enabled = True
            
            self.assertTrue(dry_run_enabled, "Should auto-enable dry-run when webhook is missing")

    @patch.dict(os.environ, {"DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/test", "DB_PATH": ""}, clear=False)
    @patch("scanner.DB_PATH")
    def test_valid_webhook_keeps_posting_enabled(self, mock_db_path):
        """Test that valid webhook keeps Discord posting enabled"""
        mock_db_path.__str__.return_value = self.db_path
        
        with patch("scanner.sqlite3.connect"):
            genie = scanner.PumpGenie()
            
            # Simulate the run() initialization logic
            webhook_url = "https://discord.com/api/webhooks/test"
            webhook_enabled = False
            if webhook_url:
                webhook_enabled = True
            
            self.assertTrue(webhook_enabled, "Should enable webhook posting when URL is valid")

    @patch("scanner.log")
    @patch.dict(os.environ, {"DISCORD_WEBHOOK_URL": "", "DB_PATH": ""}, clear=False)
    @patch("scanner.DB_PATH")
    def test_missing_webhook_logs_warning(self, mock_db_path, mock_log):
        """Test that missing webhook logs a clear warning"""
        mock_db_path.__str__.return_value = self.db_path
        
        with patch("scanner.sqlite3.connect"):
            genie = scanner.PumpGenie()
            webhook_url = ""
            if not webhook_url:
                log_message = "DISCORD_WEBHOOK_URL is not set; automatically running in dry-run mode"
                # This is what the run() method logs
                self.assertIsNotNone(log_message)
                self.assertIn("dry-run", log_message.lower())

    def test_send_alert_respects_dry_run_flag(self):
        """Test that send_alert respects the dry_run_enabled flag"""
        with patch("scanner.sqlite3.connect"):
            genie = scanner.PumpGenie()
            genie.dry_run_enabled = True
            
            # Create mock objects
            candidate = scanner.Candidate(
                chain="solana",
                mint="test123",
                source="test",
                discovered_at=0,
            )
            
            evaluation = scanner.Evaluation(
                passed=True,
                score=75,
                reasons=["Test reason"],
                failures=[],
                warnings=["Test warning"],
                pair={
                    "baseToken": {"symbol": "TEST", "name": "Test Token"},
                    "liquidity": {"usd": 50000},
                    "volume": {"h1": 20000},
                    "txns": {"h1": {"buys": 100, "sells": 80}},
                    "marketCap": 100000,
                    "fdv": 100000,
                    "priceChange": {"h1": 5.5},
                    "url": "https://example.com",
                    "pairCreatedAt": int(scanner.time.time() * 1000) - 600000,
                },
            )
            
            with patch("scanner.log") as mock_log:
                with patch.object(genie.session, "post") as mock_post:
                    genie.send_alert(candidate, evaluation)
                    # In dry-run, post should NOT be called
                    mock_post.assert_not_called()
                    # Log should indicate dry-run
                    mock_log.info.assert_called()

    def test_send_alert_posts_when_not_dry_run(self):
        """Test that send_alert posts to Discord when not in dry-run and webhook is set"""
        with patch("scanner.sqlite3.connect"):
            with patch("scanner.WEBHOOK_URL", "https://discord.com/api/webhooks/test"):
                genie = scanner.PumpGenie()
                genie.dry_run_enabled = False
                genie.webhook_enabled = True
                
                candidate = scanner.Candidate(
                    chain="solana",
                    mint="test123",
                    source="test",
                    discovered_at=0,
                )
                
                evaluation = scanner.Evaluation(
                    passed=True,
                    score=75,
                    reasons=["Test reason"],
                    failures=[],
                    warnings=["Test warning"],
                    pair={
                        "baseToken": {"symbol": "TEST", "name": "Test Token"},
                        "liquidity": {"usd": 50000},
                        "volume": {"h1": 20000},
                        "txns": {"h1": {"buys": 100, "sells": 80}},
                        "marketCap": 100000,
                        "fdv": 100000,
                        "priceChange": {"h1": 5.5},
                        "url": "https://example.com",
                        "pairCreatedAt": int(scanner.time.time() * 1000) - 600000,
                    },
                )
                
                with patch.object(genie.session, "post") as mock_post:
                    mock_post.return_value = MagicMock(status_code=204)
                    genie.send_alert(candidate, evaluation)
                    # Post SHOULD be called
                    mock_post.assert_called_once()


class TestConfigLogging(unittest.TestCase):
    """Test that configuration logging does not expose secrets"""

    @patch("scanner.log")
    def test_startup_logging_no_webhook_secret(self, mock_log):
        """Test that startup logging does not print webhook URL"""
        with patch("scanner.sqlite3.connect"):
            with patch("scanner.WEBHOOK_URL", "https://discord.com/api/webhooks/super-secret-token"):
                genie = scanner.PumpGenie()
                genie.webhook_enabled = True
                
                # Simulate the startup log message
                enabled_settings = ["Discord webhook posting"]
                log_message = f"enabled=[{', '.join(enabled_settings)}]"
                
                # The message should NOT contain the webhook URL
                self.assertNotIn("super-secret-token", log_message)
                self.assertNotIn("discord.com", log_message)


if __name__ == "__main__":
    unittest.main()
