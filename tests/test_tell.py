"""
Tests for the AVM Tell system (cross-agent messaging)
"""

import pytest
import tempfile
import os
from datetime import datetime, timezone, timedelta

from avm.tell import (
    TellStore, TellPriority, Tell, format_inbox, format_tells_for_injection,
    HookType, HookConfig, HookManager, get_hook_manager, set_hook_manager
)


@pytest.fixture
def tell_store():
    """Create a temporary TellStore for testing"""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    store = TellStore(db_path)
    yield store
    
    os.unlink(db_path)


class TestTellStore:
    """Test TellStore operations"""
    
    def test_send_and_get(self, tell_store):
        """Test sending and retrieving tells"""
        tell = tell_store.send(
            from_agent='alice',
            to_agent='bob',
            content='Hello Bob!',
            priority=TellPriority.NORMAL
        )
        
        assert tell.id is not None
        assert tell.from_agent == 'alice'
        assert tell.to_agent == 'bob'
        assert tell.content == 'Hello Bob!'
        assert tell.priority == TellPriority.NORMAL
        assert tell.read_at is None
    
    def test_get_unread(self, tell_store):
        """Test getting unread tells"""
        tell_store.send('alice', 'bob', 'Message 1')
        tell_store.send('alice', 'bob', 'Message 2')
        tell_store.send('alice', 'charlie', 'Not for bob')
        
        unread = tell_store.get_unread('bob')
        assert len(unread) == 2
        
        unread_charlie = tell_store.get_unread('charlie')
        assert len(unread_charlie) == 1
    
    def test_broadcast(self, tell_store):
        """Test @all broadcast messages"""
        tell_store.send('alice', '@all', 'Broadcast message')
        tell_store.send('alice', 'bob', 'Direct to bob')
        
        # Bob should see both
        bob_msgs = tell_store.get_unread('bob')
        assert len(bob_msgs) == 2
        
        # Charlie should see only broadcast
        charlie_msgs = tell_store.get_unread('charlie')
        assert len(charlie_msgs) == 1
        assert charlie_msgs[0].content == 'Broadcast message'
    
    def test_priority_filtering(self, tell_store):
        """Test priority-based filtering"""
        tell_store.send('alice', 'bob', 'Urgent!', priority=TellPriority.URGENT)
        tell_store.send('alice', 'bob', 'Normal', priority=TellPriority.NORMAL)
        tell_store.send('alice', 'bob', 'Low', priority=TellPriority.LOW)
        
        # Get all
        all_msgs = tell_store.get_unread('bob')
        assert len(all_msgs) == 3
        
        # Get urgent only
        urgent = tell_store.get_urgent_unread('bob')
        assert len(urgent) == 1
        assert urgent[0].content == 'Urgent!'
    
    def test_mark_read(self, tell_store):
        """Test marking tells as read"""
        t1 = tell_store.send('alice', 'bob', 'Message 1')
        t2 = tell_store.send('alice', 'bob', 'Message 2')
        
        # Mark one as read
        count = tell_store.mark_read([t1.id])
        assert count == 1
        
        # Should only have one unread now
        unread = tell_store.get_unread('bob')
        assert len(unread) == 1
        assert unread[0].id == t2.id
    
    def test_mark_all_read(self, tell_store):
        """Test marking all tells as read"""
        tell_store.send('alice', 'bob', 'Message 1')
        tell_store.send('alice', 'bob', 'Message 2')
        tell_store.send('charlie', 'bob', 'Message 3')
        
        count = tell_store.mark_all_read('bob')
        assert count == 3
        
        unread = tell_store.get_unread('bob')
        assert len(unread) == 0
    
    def test_expiration(self, tell_store):
        """Test tell expiration"""
        # Send a tell that expires in the past
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        tell_store.send('alice', 'bob', 'Expired', expires_at=past)
        
        # Send a tell that expires in the future
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        tell_store.send('alice', 'bob', 'Not expired', expires_at=future)
        
        # Only non-expired should be returned
        unread = tell_store.get_unread('bob')
        assert len(unread) == 1
        assert unread[0].content == 'Not expired'
    
    def test_get_all(self, tell_store):
        """Test getting all tells (read and unread)"""
        t1 = tell_store.send('alice', 'bob', 'Message 1')
        t2 = tell_store.send('alice', 'bob', 'Message 2')
        
        tell_store.mark_read([t1.id])
        
        all_tells = tell_store.get_all('bob')
        assert len(all_tells) == 2
    
    def test_stats(self, tell_store):
        """Test statistics"""
        t1 = tell_store.send('alice', 'bob', 'Message 1')
        tell_store.send('alice', 'bob', 'Message 2')
        tell_store.send('alice', '@all', 'Broadcast')
        
        tell_store.mark_read([t1.id])
        
        stats = tell_store.stats('bob')
        assert stats['total'] == 3  # 2 direct + 1 broadcast
        assert stats['unread'] == 2
        assert stats['read'] == 1
    
    def test_priority_ordering(self, tell_store):
        """Test that urgent messages come first"""
        tell_store.send('alice', 'bob', 'Normal', priority=TellPriority.NORMAL)
        tell_store.send('alice', 'bob', 'Urgent', priority=TellPriority.URGENT)
        tell_store.send('alice', 'bob', 'Low', priority=TellPriority.LOW)
        
        unread = tell_store.get_unread('bob')
        assert unread[0].priority == TellPriority.URGENT
        assert unread[1].priority == TellPriority.NORMAL
        assert unread[2].priority == TellPriority.LOW


class TestTellFormatting:
    """Test tell formatting functions"""
    
    def test_format_injection_header(self):
        """Test formatting tells for file injection"""
        tells = [
            Tell(
                id=1,
                from_agent='alice',
                to_agent='bob',
                content='Urgent message!',
                priority=TellPriority.URGENT,
                created_at='2026-03-09T10:00:00+00:00'
            )
        ]
        
        header = format_tells_for_injection(tells)
        
        assert '# ⚠️ UNREAD MESSAGES' in header
        assert 'alice' in header
        assert 'Urgent message!' in header
        assert '---' in header
    
    def test_format_injection_empty(self):
        """Test formatting with no tells"""
        header = format_tells_for_injection([])
        assert header == ""
    
    def test_format_inbox(self):
        """Test inbox formatting"""
        tells = [
            Tell(
                id=1,
                from_agent='alice',
                to_agent='bob',
                content='Unread message',
                priority=TellPriority.NORMAL,
                created_at='2026-03-09T10:00:00+00:00',
                read_at=None
            ),
            Tell(
                id=2,
                from_agent='charlie',
                to_agent='bob',
                content='Read message',
                priority=TellPriority.NORMAL,
                created_at='2026-03-09T09:00:00+00:00',
                read_at='2026-03-09T09:30:00+00:00'
            )
        ]
        
        inbox = format_inbox(tells, show_read=True)
        
        assert '# 📬 Inbox' in inbox
        assert 'Unread (1)' in inbox
        assert 'Read (1)' in inbox
    
    def test_format_inbox_empty(self):
        """Test inbox formatting with no messages"""
        inbox = format_inbox([])
        assert 'No messages' in inbox


class TestTellPriority:
    """Test TellPriority enum"""
    
    def test_from_string(self):
        """Test creating priority from string"""
        assert TellPriority('urgent') == TellPriority.URGENT
        assert TellPriority('normal') == TellPriority.NORMAL
        assert TellPriority('low') == TellPriority.LOW
    
    def test_to_string(self):
        """Test priority value"""
        assert TellPriority.URGENT.value == 'urgent'
        assert TellPriority.NORMAL.value == 'normal'
        assert TellPriority.LOW.value == 'low'


class TestTellDataclass:
    """Test Tell dataclass"""
    
    def test_to_dict(self):
        """Test serialization"""
        tell = Tell(
            id=1,
            from_agent='alice',
            to_agent='bob',
            content='Hello',
            priority=TellPriority.URGENT,
            created_at='2026-03-09T10:00:00+00:00'
        )
        
        d = tell.to_dict()
        assert d['priority'] == 'urgent'  # String, not enum
        assert d['from_agent'] == 'alice'
    
    def test_format_header(self):
        """Test header formatting"""
        tell = Tell(
            id=1,
            from_agent='alice',
            to_agent='bob',
            content='Important!',
            priority=TellPriority.URGENT,
            created_at='2026-03-09T10:00:00+00:00'
        )
        
        header = tell.format_header()
        assert '🔴' in header  # Urgent emoji
        assert 'alice' in header
        assert 'Important!' in header


class TestHookManager:
    """Test HookManager for tell notifications"""
    
    def test_create_manager(self):
        """Test creating empty manager"""
        manager = HookManager()
        assert manager.get_hook('bob') is None
    
    def test_register_hook(self):
        """Test registering a hook"""
        manager = HookManager()
        hook = HookConfig(type=HookType.SHELL, target="echo hello")
        manager.register('bob', hook)
        
        assert manager.get_hook('bob') is not None
        assert manager.get_hook('bob').type == HookType.SHELL
    
    def test_load_config(self):
        """Test loading hooks from config dict"""
        config = {
            'hooks': {
                'bob': {
                    'on_tell': {
                        'type': 'shell',
                        'target': 'echo ${from} said ${content}'
                    }
                },
                'alice': {
                    'on_tell': 'echo simple command'  # Simple format
                }
            }
        }
        
        manager = HookManager(config)
        
        assert manager.get_hook('bob') is not None
        assert manager.get_hook('bob').type == HookType.SHELL
        assert manager.get_hook('alice') is not None
    
    def test_trigger_shell_hook(self):
        """Test triggering a shell hook"""
        manager = HookManager()
        hook = HookConfig(type=HookType.SHELL, target="echo 'received'")
        manager.register('bob', hook)
        
        tell = Tell(
            id=1,
            from_agent='alice',
            to_agent='bob',
            content='Hello',
            priority=TellPriority.NORMAL,
            created_at='2026-03-09T10:00:00+00:00'
        )
        
        results = manager.trigger(tell)
        
        assert 'bob' in results
        assert results['bob']['success'] is True
    
    def test_trigger_broadcast(self):
        """Test triggering hooks for broadcast"""
        manager = HookManager()
        manager.register('bob', HookConfig(type=HookType.SHELL, target="echo bob"))
        manager.register('alice', HookConfig(type=HookType.SHELL, target="echo alice"))
        
        tell = Tell(
            id=1,
            from_agent='charlie',
            to_agent='@all',
            content='Broadcast',
            priority=TellPriority.NORMAL,
            created_at='2026-03-09T10:00:00+00:00'
        )
        
        results = manager.trigger(tell)
        
        assert 'bob' in results
        assert 'alice' in results
    
    def test_disabled_hook(self):
        """Test that disabled hooks are not triggered"""
        manager = HookManager()
        hook = HookConfig(type=HookType.SHELL, target="echo hello", enabled=False)
        manager.register('bob', hook)
        
        tell = Tell(
            id=1,
            from_agent='alice',
            to_agent='bob',
            content='Hello',
            priority=TellPriority.NORMAL,
            created_at='2026-03-09T10:00:00+00:00'
        )
        
        results = manager.trigger(tell)
        
        assert 'bob' not in results  # Not triggered because disabled
    
    def test_unregister_hook(self):
        """Test unregistering a hook"""
        manager = HookManager()
        hook = HookConfig(type=HookType.SHELL, target="echo hello")
        manager.register('bob', hook)
        
        assert manager.get_hook('bob') is not None
        
        manager.unregister('bob')
        
        assert manager.get_hook('bob') is None


class TestHookConfig:
    """Test HookConfig dataclass"""
    
    def test_from_string_type(self):
        """Test creating config with string type"""
        config = HookConfig(type='shell', target='echo hello')
        assert config.type == HookType.SHELL
    
    def test_defaults(self):
        """Test default values"""
        config = HookConfig(type=HookType.SHELL, target='echo')
        assert config.enabled is True
        assert config.timeout == 10
