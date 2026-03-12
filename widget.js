/**
 * AI Chatbot Embeddable Widget
 * Usage: <script src="https://yourserver.com/widget.js" data-widget-key="YOUR_KEY"></script>
 * Optional: data-color="#6366f1"  data-position="right"
 */
(function () {
  'use strict';

  const SCRIPT   = document.currentScript;
  const KEY      = SCRIPT.getAttribute('data-widget-key') || '';
  const COLOR    = SCRIPT.getAttribute('data-color') || '#6366f1';
  const POSITION = SCRIPT.getAttribute('data-position') || 'right';
  const SERVER   = SCRIPT.src.replace(/\/widget\.js(\?.*)?$/, '');

  const SIDE = POSITION === 'left' ? 'left' : 'right';

  // ── Styles ───────────────────────────────────────────────────────────────
  const css = `
    #_aichat-btn {
      position: fixed;
      bottom: 24px;
      ${SIDE}: 24px;
      width: 58px; height: 58px;
      background: ${COLOR};
      border: none;
      border-radius: 50%;
      cursor: pointer;
      box-shadow: 0 4px 24px rgba(0,0,0,0.22);
      z-index: 2147483646;
      display: flex;
      align-items: center;
      justify-content: center;
      transition: transform 0.2s, box-shadow 0.2s;
      padding: 0;
    }
    #_aichat-btn:hover {
      transform: scale(1.08);
      box-shadow: 0 6px 32px rgba(0,0,0,0.3);
    }
    #_aichat-btn svg { pointer-events: none; }

    #_aichat-window {
      position: fixed;
      bottom: 96px;
      ${SIDE}: 24px;
      width: min(400px, calc(100vw - 48px));
      height: min(620px, calc(100vh - 120px));
      border-radius: 20px;
      box-shadow: 0 12px 48px rgba(0,0,0,0.16);
      z-index: 2147483645;
      overflow: hidden;
      display: none;
      opacity: 0;
      transform: translateY(16px) scale(0.97);
      transition: opacity 0.22s ease, transform 0.22s ease;
      border: 1px solid rgba(0,0,0,0.08);
    }
    #_aichat-window.open {
      opacity: 1;
      transform: translateY(0) scale(1);
    }
    #_aichat-iframe {
      width: 100%; height: 100%;
      border: none;
      display: block;
    }

    /* Unread badge */
    #_aichat-badge {
      position: fixed;
      bottom: 68px;
      ${SIDE}: 18px;
      background: #ef4444;
      color: white;
      border-radius: 50%;
      width: 18px; height: 18px;
      font-size: 10px;
      font-weight: 700;
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 2147483647;
      font-family: -apple-system, sans-serif;
    }
  `;

  const styleEl = document.createElement('style');
  styleEl.textContent = css;
  document.head.appendChild(styleEl);

  // ── Icons ────────────────────────────────────────────────────────────────
  const ICON_CHAT = `<svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>`;
  const ICON_CLOSE = `<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>`;

  // ── DOM ──────────────────────────────────────────────────────────────────
  const btn = document.createElement('button');
  btn.id = '_aichat-btn';
  btn.setAttribute('aria-label', 'Open chat');
  btn.innerHTML = ICON_CHAT;

  const badge = document.createElement('div');
  badge.id = '_aichat-badge';
  badge.textContent = '1';

  const win = document.createElement('div');
  win.id = '_aichat-window';
  win.setAttribute('role', 'dialog');
  win.setAttribute('aria-label', 'Chat window');

  const iframe = document.createElement('iframe');
  iframe.id = '_aichat-iframe';
  iframe.title = 'AI Support Chat';
  iframe.setAttribute('allow', 'clipboard-write');
  win.appendChild(iframe);

  document.body.appendChild(win);
  document.body.appendChild(btn);
  document.body.appendChild(badge);

  // ── State ────────────────────────────────────────────────────────────────
  let isOpen       = false;
  let iframeLoaded = false;

  function open() {
    isOpen = true;
    badge.style.display = 'none';

    if (!iframeLoaded) {
      iframe.src = `${SERVER}/widget-chat?key=${encodeURIComponent(KEY)}`;
      iframeLoaded = true;
    }

    win.style.display = 'block';
    requestAnimationFrame(() => {
      requestAnimationFrame(() => win.classList.add('open'));
    });

    btn.innerHTML = ICON_CLOSE;
    btn.setAttribute('aria-label', 'Close chat');
    win.setAttribute('aria-hidden', 'false');
  }

  function close() {
    isOpen = false;
    win.classList.remove('open');
    setTimeout(() => { win.style.display = 'none'; }, 240);
    btn.innerHTML = ICON_CHAT;
    btn.setAttribute('aria-label', 'Open chat');
    win.setAttribute('aria-hidden', 'true');
  }

  btn.addEventListener('click', () => isOpen ? close() : open());

  // ── Listen for close message from iframe ─────────────────────────────────
  window.addEventListener('message', (e) => {
    if (e.data === 'aichat:close') close();
    if (e.data === 'aichat:unread') {
      if (!isOpen) badge.style.display = 'flex';
    }
  });

  // ── Keyboard: Escape to close ─────────────────────────────────────────────
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && isOpen) close();
  });

  // ── Auto-open after 30s if user hasn't opened (optional UX) ──────────────
  // Uncomment to enable:
  // setTimeout(() => { if (!isOpen && !iframeLoaded) badge.style.display = 'flex'; }, 30000);

})();
