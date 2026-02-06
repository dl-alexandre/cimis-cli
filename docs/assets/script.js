document.addEventListener('DOMContentLoaded', () => {
    initCopyButtons();
    initTabs();
    initScrollAnimations();
    initTerminalAnimation();
    animateMetricBars();
    initNavbarScroll();
});

function initCopyButtons() {
    document.querySelectorAll('.copy-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
            const text = btn.getAttribute('data-clipboard');
            try {
                await navigator.clipboard.writeText(text);
                showCopyFeedback(btn);
            } catch (err) {
                console.error('Copy failed:', err);
            }
        });
    });
}

function showCopyFeedback(button) {
    const original = button.innerHTML;
    button.classList.add('copied');
    button.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16"><path fill="currentColor" d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z"/></svg>';
    setTimeout(() => {
        button.classList.remove('copied');
        button.innerHTML = original;
    }, 2000);
}

function initTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn));
    });
}

function switchTab(clickedBtn) {
    const tabId = clickedBtn.getAttribute('data-tab');
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    clickedBtn.classList.add('active');
    document.getElementById(tabId).classList.add('active');
}

function initScrollAnimations() {
    const elements = document.querySelectorAll('.feature-card, .perf-chart, .install-card');
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.style.opacity = '1';
                entry.target.style.transform = 'translateY(0)';
            }
        });
    }, { threshold: 0.1 });

    elements.forEach(el => {
        el.style.opacity = '0';
        el.style.transform = 'translateY(30px)';
        el.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
        observer.observe(el);
    });
}

function initTerminalAnimation() {
    document.querySelectorAll('.typed').forEach(el => {
        const text = el.getAttribute('data-text');
        if (text) typeWriter(el, text);
    });
}

function typeWriter(element, text) {
    let i = 0;
    element.textContent = '';
    const type = () => {
        if (i < text.length) {
            element.textContent += text.charAt(i);
            i++;
            setTimeout(type, 50);
        }
    };
    setTimeout(type, 500);
}

function animateMetricBars() {
    const bars = document.querySelectorAll('.metric-fill, .chart-bar');
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const width = entry.target.style.width;
                entry.target.style.width = '0%';
                setTimeout(() => entry.target.style.width = width, 100);
            }
        });
    }, { threshold: 0.5 });

    bars.forEach(bar => observer.observe(bar));
}

function initNavbarScroll() {
    const nav = document.querySelector('.nav');
    window.addEventListener('scroll', () => {
        nav.style.background = window.pageYOffset > 100 
            ? 'rgba(10, 14, 10, 0.95)' 
            : 'rgba(10, 14, 10, 0.8)';
    });
}

document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
});
