/* moonkafka demo — docs site behaviour.
 *
 * Every page sets data-theme and data-lang from an inline <head> script before
 * first paint, so nothing here runs early enough to cause a flash. This file
 * only wires up the controls and keeps labels in the active language.
 */
(function () {
  "use strict";

  var root = document.documentElement;
  var THEME_KEY = "moonkafka-demo-theme";
  var LANG_KEY = "moonkafka-demo-lang";

  var TEXT = {
    zh: {
      lang: "切换到英文",
      toDark: "切换到深色模式",
      toLight: "切换到浅色模式",
      nav: "展开导航",
      copy: "复制",
      copied: "已复制"
    },
    en: {
      lang: "Switch to Chinese",
      toDark: "Switch to dark mode",
      toLight: "Switch to light mode",
      nav: "Toggle navigation",
      copy: "Copy",
      copied: "Copied"
    }
  };

  function store(key, value) {
    try {
      localStorage.setItem(key, value);
    } catch (e) {
      /* private mode: the choice just does not survive a reload */
    }
  }

  function lang() {
    return root.getAttribute("data-lang") === "en" ? "en" : "zh";
  }

  function t() {
    return TEXT[lang()];
  }

  /* ---------- theme ---------- */

  function applyTheme(theme, persist) {
    root.setAttribute("data-theme", theme);
    if (persist) {
      store(THEME_KEY, theme);
    }
    var toggle = document.querySelector(".theme-toggle");
    if (!toggle) {
      return;
    }
    var dark = theme === "dark";
    var label = dark ? t().toLight : t().toDark;
    toggle.textContent = dark ? "☀" : "☽";
    toggle.setAttribute("aria-label", label);
    toggle.setAttribute("title", label);
  }

  /* ---------- language ---------- */

  function applyLang(next, persist) {
    root.setAttribute("data-lang", next);
    root.setAttribute("lang", next === "en" ? "en" : "zh-CN");
    if (persist) {
      store(LANG_KEY, next);
    }

    var buttons = document.querySelectorAll("[data-lang-set]");
    for (var i = 0; i < buttons.length; i++) {
      var on = buttons[i].getAttribute("data-lang-set") === next;
      buttons[i].setAttribute("aria-pressed", String(on));
    }

    var switchEl = document.querySelector(".lang-switch");
    if (switchEl) {
      switchEl.setAttribute("aria-label", t().lang);
    }

    var navToggle = document.querySelector(".nav-toggle");
    if (navToggle) {
      navToggle.setAttribute("aria-label", t().nav);
      navToggle.setAttribute("title", t().nav);
    }

    // Copy buttons carry their label in the DOM, so they follow the switch too.
    var copies = document.querySelectorAll(".copy-btn");
    for (var j = 0; j < copies.length; j++) {
      if (!copies[j].classList.contains("copied")) {
        copies[j].textContent = t().copy;
      }
    }

    applyTheme(root.getAttribute("data-theme") === "dark" ? "dark" : "light", false);
  }

  /* ---------- copy buttons ---------- */

  function addCopyButtons() {
    var blocks = document.querySelectorAll("pre");
    for (var i = 0; i < blocks.length; i++) {
      (function (pre) {
        var wrap = document.createElement("div");
        wrap.className = "pre-block";
        pre.parentNode.insertBefore(wrap, pre);
        wrap.appendChild(pre);

        var button = document.createElement("button");
        button.type = "button";
        button.className = "copy-btn";
        button.textContent = t().copy;
        button.addEventListener("click", function () {
          var text = pre.innerText;
          var done = function () {
            button.textContent = t().copied;
            button.classList.add("copied");
            window.setTimeout(function () {
              button.textContent = t().copy;
              button.classList.remove("copied");
            }, 1400);
          };
          if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(done, function () {});
          } else {
            // Older or non-secure contexts: fall back to a selection copy.
            var area = document.createElement("textarea");
            area.value = text;
            area.setAttribute("readonly", "");
            area.style.position = "fixed";
            area.style.opacity = "0";
            document.body.appendChild(area);
            area.select();
            try {
              document.execCommand("copy");
              done();
            } catch (e) {
              /* nothing to do: the user can select the code by hand */
            }
            document.body.removeChild(area);
          }
        });
        wrap.appendChild(button);
      })(blocks[i]);
    }
  }

  /* ---------- wiring ---------- */

  document.addEventListener("DOMContentLoaded", function () {
    var langButtons = document.querySelectorAll("[data-lang-set]");
    for (var i = 0; i < langButtons.length; i++) {
      langButtons[i].addEventListener("click", function () {
        applyLang(this.getAttribute("data-lang-set"), true);
      });
    }

    var themeToggle = document.querySelector(".theme-toggle");
    if (themeToggle) {
      themeToggle.addEventListener("click", function () {
        applyTheme(root.getAttribute("data-theme") === "dark" ? "light" : "dark", true);
      });
    }

    var navToggle = document.querySelector(".nav-toggle");
    var sidebar = document.querySelector(".sidebar");
    if (navToggle && sidebar) {
      navToggle.addEventListener("click", function () {
        var open = sidebar.classList.toggle("open");
        navToggle.setAttribute("aria-expanded", String(open));
      });
    }

    addCopyButtons();
    applyLang(lang(), false);

    // Follow the system only while the reader has not made a choice.
    var media = window.matchMedia("(prefers-color-scheme: dark)");
    if (media.addEventListener) {
      media.addEventListener("change", function (event) {
        var chosen = null;
        try {
          chosen = localStorage.getItem(THEME_KEY);
        } catch (e) {}
        if (chosen !== "light" && chosen !== "dark") {
          applyTheme(event.matches ? "dark" : "light", false);
        }
      });
    }
  });
})();
