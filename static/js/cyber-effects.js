/**
 * cyber-effects.js
 * React Bits — Vanilla JS Implementation for DB Tuner Pro
 *
 * Includes:
 *  1. Particle Canvas Background (React Bits: Particles)
 *  2. Animated Grid Overlay
 *  3. Card Tilt / Magnetic Hover (React Bits: MagneticButton / TiltCard)
 *  4. Typing Text Effect (React Bits: DecryptedText / TypingAnimation)
 *  5. Neon Cursor Trail
 *  6. Loading Skeleton Generator
 *  7. Counter animation (React Bits: NumberFlow)
 *  8. GradualBlur (already in gradual-blur.js, referenced here)
 */
(function () {
  'use strict';

  /* ─── 1. PARTICLE CANVAS BACKGROUND ──────────────────────── */
  function initParticles() {
    const canvas = document.getElementById('cyberCanvas');
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    function resize() {
      canvas.width  = window.innerWidth;
      canvas.height = window.innerHeight;
    }
    resize();
    window.addEventListener('resize', resize);

    const PARTICLE_COUNT = 60;
    const COLORS = ['rgba(0,240,255,', 'rgba(157,0,255,', 'rgba(255,0,200,'];

    const particles = Array.from({ length: PARTICLE_COUNT }, () => ({
      x    : Math.random() * window.innerWidth,
      y    : Math.random() * window.innerHeight,
      r    : Math.random() * 1.4 + 0.3,
      vx   : (Math.random() - 0.5) * 0.35,
      vy   : (Math.random() - 0.5) * 0.35,
      alpha: Math.random() * 0.5 + 0.15,
      color: COLORS[Math.floor(Math.random() * COLORS.length)],
    }));

    const CONNECTION_DIST = 100;

    function draw() {
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      /* Grid */
      ctx.strokeStyle = 'rgba(0,240,255,0.03)';
      ctx.lineWidth   = 1;
      const GRID = 36;
      for (let x = 0; x < canvas.width; x += GRID) {
        ctx.beginPath();
        ctx.moveTo(x, 0); ctx.lineTo(x, canvas.height);
        ctx.stroke();
      }
      for (let y = 0; y < canvas.height; y += GRID) {
        ctx.beginPath();
        ctx.moveTo(0, y); ctx.lineTo(canvas.width, y);
        ctx.stroke();
      }

      /* Particles + connections */
      particles.forEach((p, i) => {
        p.x += p.vx;
        p.y += p.vy;
        if (p.x < 0) p.x = canvas.width;
        if (p.x > canvas.width)  p.x = 0;
        if (p.y < 0) p.y = canvas.height;
        if (p.y > canvas.height) p.y = 0;

        /* Draw particle */
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fillStyle = p.color + p.alpha + ')';
        ctx.fill();

        /* Draw connections */
        for (let j = i + 1; j < particles.length; j++) {
          const q    = particles[j];
          const dist = Math.hypot(p.x - q.x, p.y - q.y);
          if (dist < CONNECTION_DIST) {
            const a = (1 - dist / CONNECTION_DIST) * 0.07;
            ctx.beginPath();
            ctx.moveTo(p.x, p.y);
            ctx.lineTo(q.x, q.y);
            ctx.strokeStyle = `rgba(0,240,255,${a})`;
            ctx.lineWidth   = 0.5;
            ctx.stroke();
          }
        }
      });

      requestAnimationFrame(draw);
    }
    draw();
  }

  /* ─── 2. CARD TILT (React Bits: TiltCard) ────────────────── */
  function initCardTilt() {
    document.querySelectorAll('.action-card').forEach(card => {
      card.addEventListener('mousemove', e => {
        const rect   = card.getBoundingClientRect();
        const cx     = rect.left + rect.width  / 2;
        const cy     = rect.top  + rect.height / 2;
        const dx     = (e.clientX - cx) / (rect.width  / 2);
        const dy     = (e.clientY - cy) / (rect.height / 2);
        card.style.transform = `perspective(600px) rotateY(${dx * 5}deg) rotateX(${-dy * 5}deg) translateY(-3px)`;
      });
      card.addEventListener('mouseleave', () => {
        card.style.transform = '';
        card.style.transition = 'transform 0.5s ease';
      });
      card.addEventListener('mouseenter', () => {
        card.style.transition = 'transform 0.1s ease, border-color 0.4s ease, box-shadow 0.4s ease';
      });
    });
  }

  /* ─── 3. MAGNETIC BUTTON PULSE (React Bits: MagneticButton) ─ */
  function initMagneticButtons() {
    document.querySelectorAll('.btn-primary, .btn-ghost').forEach(btn => {
      btn.addEventListener('mousemove', e => {
        const rect = btn.getBoundingClientRect();
        const x    = e.clientX - rect.left - rect.width  / 2;
        const y    = e.clientY - rect.top  - rect.height / 2;
        btn.style.transform = `translate(${x * 0.15}px, ${y * 0.15}px)`;
      });
      btn.addEventListener('mouseleave', () => {
        btn.style.transform = '';
        btn.style.transition = 'all 0.3s ease';
      });
    });
  }

  /* ─── 4. DECRYPTED / TYPING TEXT (React Bits: DecryptedText) */
  const CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@#$%&';
  function decryptText(el, finalText, duration = 800) {
    let frame = 0;
    const totalFrames = Math.floor(duration / 16);
    const id = setInterval(() => {
      el.textContent = finalText
        .split('')
        .map((ch, i) => {
          if (ch === ' ') return ' ';
          if (frame / totalFrames > i / finalText.length) return ch;
          return CHARS[Math.floor(Math.random() * CHARS.length)];
        })
        .join('');
      if (++frame >= totalFrames) {
        clearInterval(id);
        el.textContent = finalText;
      }
    }, 16);
  }

  function initDecryptedText() {
    /* Animate the logo text on load */
    const logo = document.querySelector('.logo h2');
    if (logo) {
      const original = logo.textContent.trim();
      setTimeout(() => decryptText(logo, original, 1000), 300);
    }
    /* Animate topbar h1 on each page */
    const h1 = document.querySelector('.topbar-title h1');
    if (h1) {
      const orig = h1.textContent.trim();
      setTimeout(() => decryptText(h1, orig, 700), 500);
    }
  }

  /* ─── 5. NEON CURSOR TRAIL (React Bits: Cursor) ─────────── */
  function initCursorTrail() {
    const trail = [];
    const TRAIL_LEN = 8;
    for (let i = 0; i < TRAIL_LEN; i++) {
      const dot = document.createElement('div');
      Object.assign(dot.style, {
        position      : 'fixed',
        pointerEvents : 'none',
        borderRadius  : '50%',
        zIndex        : '9998',
        transform     : 'translate(-50%, -50%)',
        transition    : `opacity 0.3s ease`,
        background    : 'var(--neon-blue)',
      });
      document.body.appendChild(dot);
      trail.push({ el: dot, x: 0, y: 0 });
    }

    let mx = 0, my = 0;
    document.addEventListener('mousemove', e => { mx = e.clientX; my = e.clientY; });

    function animate() {
      let lx = mx, ly = my;
      trail.forEach((t, i) => {
        const alpha  = 1 - i / TRAIL_LEN;
        const size   = Math.max(1, (TRAIL_LEN - i) * 0.8);
        t.x += (lx - t.x) * 0.35;
        t.y += (ly - t.y) * 0.35;
        Object.assign(t.el.style, {
          left    : `${t.x}px`,
          top     : `${t.y}px`,
          width   : `${size}px`,
          height  : `${size}px`,
          opacity : alpha * 0.5,
          boxShadow: `0 0 ${size * 2}px rgba(0,240,255,${alpha * 0.8})`,
        });
        lx = t.x; ly = t.y;
      });
      requestAnimationFrame(animate);
    }
    animate();
  }

  /* ─── 6. NUMBER FLOW / COUNTER (React Bits: NumberFlow) ────- */
  function animateCounter(el, target, duration = 1200) {
    const start = performance.now();
    const from  = parseFloat(el.textContent.replace(/,/g, '')) || 0;
    const to    = parseFloat(String(target).replace(/,/g, ''));
    if (isNaN(to)) return;
    function frame(now) {
      const t = Math.min((now - start) / duration, 1);
      const ease = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
      el.textContent = Math.round(from + (to - from) * ease);
      if (t < 1) requestAnimationFrame(frame);
      else el.textContent = target;
    }
    requestAnimationFrame(frame);
  }

  /* Observe stat values and animate when they change */
  function initCounterAnimation() {
    const targets = ['valTotalQueries', 'valTotalIssues', 'valTotalRecs',
                     'fpUniqueTemplates', 'fpTotalExec', 'fpTotalRows'];
    targets.forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      const observer = new MutationObserver(() => {
        animateCounter(el, el.textContent, 800);
      });
      observer.observe(el, { childList: true, characterData: true, subtree: true });
    });
  }

  /* ─── 7. SKELETON LOADERS (React Bits: Skeleton) ──────────── */
  function showSkeletons(container, count = 3) {
    container.innerHTML = '';
    for (let i = 0; i < count; i++) {
      const sk = document.createElement('div');
      sk.className = 'skeleton-row';
      Object.assign(sk.style, {
        height       : '48px',
        borderRadius : '8px',
        marginBottom : '6px',
        background   : 'linear-gradient(90deg, rgba(0,240,255,0.04) 25%, rgba(0,240,255,0.08) 50%, rgba(0,240,255,0.04) 75%)',
        backgroundSize: '200% 100%',
        animation    : 'skeletonShimmer 1.5s ease-in-out infinite',
      });
      container.appendChild(sk);
    }
  }

  /* Add keyframe for skeleton */
  function injectSkeletonAnimation() {
    if (document.getElementById('skeletonStyle')) return;
    const s = document.createElement('style');
    s.id = 'skeletonStyle';
    s.textContent = `
      @keyframes skeletonShimmer {
        0%   { background-position:  200% 0; }
        100% { background-position: -200% 0; }
      }`;
    document.head.appendChild(s);
  }

  /* Override loading panel to use our skeleton */
  function hookLoadingState() {
    const loadingPanel = document.getElementById('loadingState');
    if (!loadingPanel) return;
    const origShow = window.showLoading;
    if (origShow) return; // already hooked
    /* Observe class changes on loadingPanel */
    const obs = new MutationObserver(() => {
      if (!loadingPanel.classList.contains('hidden')) {
        const list = document.getElementById('healthItemsList');
        if (list) showSkeletons(list, 4);
      }
    });
    obs.observe(loadingPanel, { attributeFilter: ['class'] });
  }

  /* ─── 8. HOVER SHIMMER ON PROJECT ROWS ──────────────────── */
  function initRowShimmer() {
    document.addEventListener('mouseover', e => {
      const row = e.target.closest('.project-row');
      if (!row) return;
      if (row._shimmerActive) return;
      row._shimmerActive = true;
      setTimeout(() => { row._shimmerActive = false; }, 400);
    });
  }

  /* ─── 9. STAGGER FADE-IN FOR LISTS ─────────────────────── */
  function staggerFadeIn(parentSelector) {
    const parent = document.querySelector(parentSelector);
    if (!parent) return;
    const observer = new MutationObserver(mutations => {
      mutations.forEach(m => {
        m.addedNodes.forEach((node, i) => {
          if (node.nodeType !== 1) return;
          node.style.opacity   = '0';
          node.style.transform = 'translateY(8px)';
          setTimeout(() => {
            node.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
            node.style.opacity    = '1';
            node.style.transform  = 'translateY(0)';
          }, i * 60 + 50);
        });
      });
    });
    observer.observe(parent, { childList: true });
  }

  /* ─── INIT ALL ─────────────────────────────────────────── */
  function init() {
    injectSkeletonAnimation();
    initParticles();
    initCardTilt();
    initMagneticButtons();
    initDecryptedText();
    initCursorTrail();
    initCounterAnimation();
    hookLoadingState();
    initRowShimmer();
    staggerFadeIn('#healthItemsList');
    staggerFadeIn('#recommendationsList');
    staggerFadeIn('#fpTemplatesList');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  /* Re-init on dynamic content loads */
  window.addEventListener('analysisComplete', () => {
    initCardTilt();
    initMagneticButtons();
  });
})();
