---
name: Entropy Arb
description: A precise night-shift operations system for a single arbitrage strategy.
colors:
  night-slate: "#071018"
  working-surface: "#0a151e"
  raised-surface: "#0d1a24"
  graphite-rule: "#1c2c37"
  strong-rule: "#2b3d48"
  measurement-white: "#dce6e9"
  secondary-reading: "#81949e"
  faint-reading: "#5f737e"
  datum-blue: "#6aa1b4"
  warning-amber: "#d1a15d"
  healthy-green: "#69b38e"
  error-red: "#d97870"
typography:
  title:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
    fontSize: "18px"
    fontWeight: 640
    lineHeight: 1.2
    letterSpacing: "-0.015em"
  body:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
    fontSize: "11px"
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontFamily: "Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
    fontSize: "10px"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "0.08em"
rounded:
  control: "4px"
  tag: "3px"
  status: "999px"
spacing:
  xs: "8px"
  sm: "12px"
  md: "16px"
  lg: "24px"
  section: "34px"
components:
  range-button:
    backgroundColor: "transparent"
    textColor: "{colors.secondary-reading}"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    height: "30px"
  range-button-selected:
    backgroundColor: "#17303d"
    textColor: "#bcd5dc"
    typography: "{typography.label}"
    rounded: "{rounded.control}"
    height: "30px"
  warning-banner:
    backgroundColor: "#17150f"
    textColor: "#e6c38d"
    padding: "9px 14px"
---

# Design System: Entropy Arb

## Overview

**Creative North Star: "The Night Tide-Observation Station"**

Entropy Arb is an instrument panel for one operator watching one live strategy for long periods. Its visual world borrows from hydrographic observation: premium is a tide trace, the dynamic midline is a datum, and entry and exit thresholds are warning levels. The interface is quiet, dark, and exact so market state stays readable without theatrical finance styling.

The system is deliberately structural. Rules separate information; wide open plotting fields establish hierarchy; color appears only when it carries market, health, or risk meaning.

**Key Characteristics:**

- Near-black blue slate with graphite measurement rules.
- One dominant data trace, followed by compact operational detail.
- Tabular numerals and restrained uppercase labels.
- Amber for attention, green and red only for explicit outcomes.
- Flat surfaces with minimal corners and no floating card grid.

## Colors

The palette is a cool, low-luminance field with off-white measurements and sparse semantic signals.

### Primary

- **Datum Blue:** The model midline, selected controls, and analytical reference state.

### Secondary

- **Warning Amber:** Stale data, blocked exposure, HEDGE actions, and attention states.
- **Healthy Green:** Running, ready, and successful execution states only.
- **Error Red:** Engine errors, failed executions, and unsafe delta only.

### Neutral

- **Night Slate:** The continuous page background.
- **Working Surface / Raised Surface:** Subtle tonal separation inside controls and analytical details.
- **Measurement White:** Primary values and headings.
- **Secondary / Faint Reading:** Explanations, metadata, axes, and lower-priority labels.
- **Graphite Rules:** Section boundaries and table structure.

### Named Rules

**The Semantic Signal Rule.** Green, amber, and red never decorate; each must explain an operational state.

## Typography

**Display Font:** Inter with the system sans stack
**Body Font:** Inter with the system sans stack
**Label/Mono Font:** The same sans stack; tabular numeral features supply measurement rhythm.

**Character:** Compact and neutral, with weight and spacing—not multiple font families—creating hierarchy. The implementation follows the product specification's Inter/system requirement.

### Hierarchy

- **Title** (640, 16–18px): Major analytical regions.
- **Metric** (560, 20–29px): Live KPI readings with tabular numerals.
- **Body** (400, 11px, 1.5): Explanations and operational copy, capped near 72 characters where applicable.
- **Label** (650–750, 9–10px, 0.06–0.12em, uppercase): Controls, table headers, tags, and measurement names.

### Named Rules

**The Reading Before Branding Rule.** Type must make values easier to compare before it expresses personality.

## Layout

The desktop container is capped at 1440px with 32px page gutters. A 64px global status rail precedes four divided KPIs, then the full-width premium plot. Strategy and market details share one ruled row; PnL and trades continue in strict reading order.

At 800px and below, KPIs become a two-by-two rail and Strategy and Market stack. At 460px, gutters contract to 16px, chart controls tighten, and the trades table scrolls horizontally. The dashboard must not create page-level horizontal overflow at 390px.

## Elevation & Depth

The system is flat by default. Tonal layering and one-pixel rules establish depth; shadows are reserved for transient chart tooltips and low-radius status glow. Wide resting surfaces do not combine borders and shadows.

### Shadow Vocabulary

- **Tooltip Lift** (`0 10px 28px rgba(0,0,0,.28)`): Only for information floating above a plot.
- **Status Glow** (`0 0 10px` at low opacity): Only around the small live-status dot.

**The Instrument Plane Rule.** Operational content stays on one continuous plane unless it must visibly float above data.

## Shapes

Controls and tags use compact 3–4px corners. Circular geometry belongs only to status dots and the warning marker. Content regions use straight rules rather than rounded containers; pills are not section scaffolding.

## Components

### Range Buttons

- **Shape:** Compact squared control (4px outer radius), 30px high.
- **Default:** Transparent field, secondary text, one-pixel graphite separators.
- **Selected:** Tonal datum-blue field and brighter blue-white text.
- **Hover / Focus:** Slight surface lift on hover; a visible 2px datum-blue focus ring.

### Status Chips

- **Style:** Text and a 6px semantic dot, without a filled pill background.
- **State:** Green for running, amber for stale or warning.

### KPI Rail

- **Style:** Four measurements share one ruled band; vertical rules separate columns.
- **Hierarchy:** Uppercase label, primary tabular value, then muted supporting context.
- **Responsive:** Two-by-two at mobile widths.

### Warning Banner

- **Style:** Amber-tinted near-black surface, one-pixel amber rule, and a circular exclamation marker.
- **Copy:** Names the exact condition and whether reductions remain possible.

### Detail Panels

- **Style:** Flat two-column analytical row, not cards. Thresholds form a five-stop datum strip.
- **Responsive:** Panels stack and acquire a horizontal divider on mobile.

### Trade Table

- **Style:** Dense ruled rows with neutral action text. HEDGE alone is amber; REDUCE remains neutral.
- **State:** Success uses green, partial uses amber, and error uses red.

## Do's and Don'ts

### Do:

- **Do** let the premium chart dominate the first viewport.
- **Do** use `--` when an accounting value is unknown or unreliable.
- **Do** keep thresholds in the same premium coordinate system as the plotted trace.
- **Do** preserve keyboard focus, tabular numerals, themed selection, and responsive stacking.

### Don't:

- **Don't** turn sections into a grid of floating rounded cards.
- **Don't** infer PnL or other product facts in the browser.
- **Don't** use green or red for ordinary direction labels or REDUCE actions.
- **Don't** add decorative gradients, finance clichés, side navigation, or ornamental sparklines.
