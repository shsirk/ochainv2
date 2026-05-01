export function showSkeleton(container, rows = 5) {
    const wrap = document.createElement('div');
    wrap.className = 'skeleton-wrap';
    for (let i = 0; i < rows; i++) {
        const d = document.createElement('div');
        d.className = 'skeleton';
        wrap.appendChild(d);
    }
    container.innerHTML = '';
    container.appendChild(wrap);
}

export function showSkeletonCharts(container, count = 3) {
    const wrap = document.createElement('div');
    wrap.className = 'skeleton-wrap';
    wrap.style.display = 'grid';
    wrap.style.gridTemplateColumns = `repeat(${count},1fr)`;
    wrap.style.gap = '0.6rem';
    for (let i = 0; i < count; i++) {
        const d = document.createElement('div');
        d.className = 'skeleton-chart';
        wrap.appendChild(d);
    }
    container.innerHTML = '';
    container.appendChild(wrap);
}

export function hideSkeleton(container) {
    const w = container.querySelector('.skeleton-wrap');
    if (w) w.remove();
}
