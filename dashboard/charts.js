// Minimal rolling multi-series line chart on canvas: 2px lines, hairline
// grid, autoscaled single y-axis, crosshair + tooltip hover layer. No
// dependencies; the page stays self-contained.

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

export class RollingChart {
  /**
   * @param {HTMLCanvasElement} canvas
   * @param {HTMLElement} legendEl
   * @param {{series: {name: string, color: string}[], maxPoints?: number,
   *          yMin?: number, format?: (v: number) => string}} opts
   */
  constructor(canvas, legendEl, opts) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d");
    this.series = opts.series;
    this.maxPoints = opts.maxPoints ?? 180;
    this.yMin = opts.yMin ?? 0;
    this.format = opts.format ?? ((v) => formatNum(v));
    this.points = []; // {t: ms, v: number[]}
    this.tooltip = document.getElementById("tooltip");
    this.hoverX = null;

    this.legendItems = this.series.map((s) => {
      const key = document.createElement("span");
      key.className = "key";
      const swatch = document.createElement("span");
      swatch.className = "swatch";
      swatch.style.background = s.color;
      const label = document.createElement("span");
      label.textContent = s.name;
      const val = document.createElement("span");
      val.className = "val";
      val.textContent = "";
      key.append(swatch, label, val);
      legendEl.append(key);
      return val;
    });

    canvas.addEventListener("mousemove", (e) => this.onHover(e));
    canvas.addEventListener("mouseleave", () => {
      this.hoverX = null;
      this.tooltip.hidden = true;
      this.draw();
    });
  }

  push(t, values) {
    this.points.push({ t, v: values });
    if (this.points.length > this.maxPoints) this.points.shift();
    values.forEach((v, i) => {
      if (this.legendItems[i]) this.legendItems[i].textContent = v == null ? "" : this.format(v);
    });
    this.scheduleDraw();
  }

  scheduleDraw() {
    if (this.raf) return;
    this.raf = requestAnimationFrame(() => {
      this.raf = null;
      this.draw();
    });
  }

  layout() {
    const dpr = window.devicePixelRatio || 1;
    const w = this.canvas.clientWidth;
    const h = this.canvas.clientHeight;
    if (this.canvas.width !== w * dpr || this.canvas.height !== h * dpr) {
      this.canvas.width = w * dpr;
      this.canvas.height = h * dpr;
    }
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return { w, h, padL: 46, padR: 8, padT: 8, padB: 18 };
  }

  bounds() {
    let max = -Infinity;
    let min = Infinity;
    for (const p of this.points) {
      for (const v of p.v) {
        if (v == null) continue;
        if (v > max) max = v;
        if (v < min) min = v;
      }
    }
    if (!isFinite(max)) { max = 1; min = 0; }
    min = Math.min(this.yMin, min);
    if (max - min < 1e-9) max = min + 1;
    max += (max - min) * 0.08; // headroom
    return { min, max };
  }

  draw() {
    const { w, h, padL, padR, padT, padB } = this.layout();
    const { min, max } = this.bounds();
    const ctx = this.ctx;
    const plotW = w - padL - padR;
    const plotH = h - padT - padB;
    ctx.clearRect(0, 0, w, h);
    if (this.points.length === 0) return;

    const t0 = this.points[0].t;
    const t1 = this.points[this.points.length - 1].t;
    const spanT = Math.max(t1 - t0, 1000);
    const x = (t) => padL + ((t - t0) / spanT) * plotW;
    const y = (v) => padT + plotH - ((v - min) / (max - min)) * plotH;

    // Hairline grid + tick labels (muted, tabular).
    ctx.strokeStyle = css("--grid");
    ctx.fillStyle = css("--muted");
    ctx.lineWidth = 1;
    ctx.font = "11px " + css("--font");
    ctx.textAlign = "right";
    const ticks = 4;
    for (let i = 0; i <= ticks; i++) {
      const v = min + ((max - min) * i) / ticks;
      const yy = Math.round(y(v)) + 0.5;
      ctx.beginPath();
      ctx.moveTo(padL, yy);
      ctx.lineTo(w - padR, yy);
      ctx.stroke();
      ctx.fillText(this.format(v), padL - 6, yy + 3);
    }
    // Time axis: seconds-ago marks.
    ctx.textAlign = "center";
    for (const secsAgo of [120, 60, 0]) {
      const tt = t1 - secsAgo * 1000;
      if (tt < t0) continue;
      ctx.fillText(secsAgo === 0 ? "now" : `-${secsAgo}s`, x(tt), h - 4);
    }

    // Series: 2px lines, no point markers (the hover layer reads values).
    for (let s = 0; s < this.series.length; s++) {
      ctx.strokeStyle = this.series[s].color;
      ctx.lineWidth = 2;
      ctx.lineJoin = "round";
      ctx.beginPath();
      let started = false;
      for (const p of this.points) {
        const v = p.v[s];
        if (v == null) { started = false; continue; }
        const xx = x(p.t);
        const yy = y(v);
        if (!started) { ctx.moveTo(xx, yy); started = true; }
        else ctx.lineTo(xx, yy);
      }
      ctx.stroke();
    }

    // Crosshair.
    if (this.hoverX != null) {
      const idx = this.nearestIndex(this.hoverX, x);
      if (idx != null) {
        const p = this.points[idx];
        const xx = Math.round(x(p.t)) + 0.5;
        ctx.strokeStyle = css("--baseline");
        ctx.beginPath();
        ctx.moveTo(xx, padT);
        ctx.lineTo(xx, padT + plotH);
        ctx.stroke();
        for (let s = 0; s < this.series.length; s++) {
          const v = p.v[s];
          if (v == null) continue;
          ctx.fillStyle = this.series[s].color;
          ctx.beginPath();
          ctx.arc(x(p.t), y(v), 3.5, 0, Math.PI * 2);
          ctx.fill();
        }
      }
    }
  }

  nearestIndex(clientX, xScale) {
    const rect = this.canvas.getBoundingClientRect();
    const px = clientX - rect.left;
    let best = null;
    let bestD = Infinity;
    for (let i = 0; i < this.points.length; i++) {
      const d = Math.abs(xScale(this.points[i].t) - px);
      if (d < bestD) { bestD = d; best = i; }
    }
    return best;
  }

  onHover(e) {
    this.hoverX = e.clientX;
    this.scheduleDraw();
    const { w, h, padL, padR } = this.layout();
    void w; void h; void padR;
    const t0 = this.points[0]?.t;
    if (t0 == null) return;
    const t1 = this.points[this.points.length - 1].t;
    const spanT = Math.max(t1 - t0, 1000);
    const plotW = this.canvas.clientWidth - padL - 8;
    const idx = this.nearestIndex(e.clientX, (t) => padL + ((t - t0) / spanT) * plotW);
    if (idx == null) return;
    const p = this.points[idx];
    const secsAgo = Math.round((t1 - p.t) / 1000);
    const rows = this.series
      .map((s, i) =>
        p.v[i] == null
          ? ""
          : `<div class="row"><span class="swatch" style="background:${s.color};width:8px;height:8px;border-radius:2px"></span>${s.name}<span class="num">${this.format(p.v[i])}</span></div>`
      )
      .join("");
    this.tooltip.innerHTML = `<div class="row" style="color:var(--muted)">${secsAgo === 0 ? "now" : `-${secsAgo}s`}</div>${rows}`;
    this.tooltip.hidden = false;
    const tw = this.tooltip.offsetWidth;
    const left = e.clientX + 14 + tw > window.innerWidth ? e.clientX - tw - 14 : e.clientX + 14;
    this.tooltip.style.left = `${left}px`;
    this.tooltip.style.top = `${e.clientY + 12}px`;
  }
}

export function formatNum(v) {
  if (v == null || !isFinite(v)) return "";
  const a = Math.abs(v);
  if (a >= 1_000_000) return (v / 1_000_000).toFixed(1) + "M";
  if (a >= 10_000) return (v / 1000).toFixed(1) + "k";
  if (a >= 100) return Math.round(v).toString();
  if (a >= 10) return v.toFixed(1);
  return v.toFixed(2).replace(/\.?0+$/, "") || "0";
}
