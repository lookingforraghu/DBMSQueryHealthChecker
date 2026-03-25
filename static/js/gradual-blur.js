/**
 * GradualBlur — Vanilla JS Port
 * Original React component by Ansh Dhanani — github.com/ansh-dhanani
 * Ported to vanilla JS for Flask/Jinja2 projects
 *
 * z-index strategy:
 *  - Blur wrap sits at z-index 200 (ABOVE all content visually)
 *  - pointer-events: none on every element so ALL clicks pass through
 *  - backdrop-filter blurs whatever is behind it, creating the fade
 */
(function () {
  'use strict';

  const CURVE = {
    linear:        p => p,
    bezier:        p => p * p * (3 - 2 * p),
    'ease-in':     p => p * p,
    'ease-out':    p => 1 - Math.pow(1 - p, 2),
    'ease-in-out': p => p < 0.5 ? 2 * p * p : 1 - Math.pow(-2 * p + 2, 2) / 2,
  };

  const DIR = { top: 'to top', bottom: 'to bottom', left: 'to left', right: 'to right' };

  function createGradualBlur(parentEl, cfg = {}) {
    const c = Object.assign({
      position:    'bottom',
      strength:    2,
      height:      '7rem',
      divCount:    6,
      exponential: true,
      opacity:     1,
      curve:       'bezier',
      zIndex:      200,  // HIGH — visually above content; pointer-events:none so nothing is blocked
    }, cfg);

    const curveF    = CURVE[c.curve] || CURVE.linear;
    const dir       = DIR[c.position] || 'to bottom';
    const increment = 100 / c.divCount;
    const isVert    = ['top', 'bottom'].includes(c.position);

    const wrap = document.createElement('div');
    wrap.className = 'gb-wrap';
    Object.assign(wrap.style, {
      position:      'absolute',
      pointerEvents: 'none',
      zIndex:         c.zIndex,
      [c.position]:  '0',
      left:   isVert  ? '0' : undefined,
      right:  isVert  ? '0' : undefined,
      top:    !isVert ? '0' : undefined,
      bottom: !isVert ? '0' : undefined,
      width:  isVert  ? '100%' : c.height,
      height: isVert  ? c.height : '100%',
    });

    for (let i = 1; i <= c.divCount; i++) {
      const p      = curveF(i / c.divCount);
      const blurVal = c.exponential
        ? Math.pow(2, p * 4) * 0.0625 * c.strength
        : 0.0625 * (p * c.divCount + 1) * c.strength;

      const p1 = Math.round((increment * i - increment) * 10) / 10;
      const p2 = Math.round( increment * i              * 10) / 10;
      const p3 = Math.round((increment * i + increment) * 10) / 10;
      const p4 = Math.round((increment * i + increment * 2) * 10) / 10;

      let grad = `transparent ${p1}%, black ${p2}%`;
      if (p3 <= 100) grad += `, black ${p3}%`;
      if (p4 <= 100) grad += `, transparent ${p4}%`;

      const mask = `linear-gradient(${dir}, ${grad})`;
      const div  = document.createElement('div');

      Object.assign(div.style, {
        position:              'absolute',
        inset:                 '0',
        maskImage:              mask,
        WebkitMaskImage:        mask,
        backdropFilter:        `blur(${blurVal.toFixed(3)}rem)`,
        WebkitBackdropFilter:  `blur(${blurVal.toFixed(3)}rem)`,
        opacity:                c.opacity,
        pointerEvents:         'none',
      });

      wrap.appendChild(div);
    }

    if (getComputedStyle(parentEl).position === 'static') {
      parentEl.style.position = 'relative';
    }

    parentEl.appendChild(wrap);
    return wrap;
  }

  window.GradualBlur = { create: createGradualBlur };

  function autoInit() {
    const scroll = document.querySelector('.content-scroll');
    if (scroll) {
      createGradualBlur(scroll, {
        position:    'bottom',
        height:      '7rem',
        strength:    2.5,
        divCount:    6,
        curve:       'bezier',
        exponential: true,
        opacity:     1,
        zIndex:      200,
      });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', autoInit);
  } else {
    autoInit();
  }
})();
