import type { ReactNode } from "react";

type Props = {
  sidebar: ReactNode;
  topbar: ReactNode;
  children: ReactNode;
};

export default function AppShell({ sidebar, topbar, children }: Props) {
  return (
    <div className="app-shell">

      {/* Sidebar */}
      <aside className="app-sidebar">
        {sidebar}
      </aside>

      {/* Main dashboard area */}
      <div className="app-main">

        {/* Top navigation bar */}
        <header className="app-topbar">
          {topbar}
        </header>

        {/* Page content */}
        <main className="app-content">
          {children}
        </main>

      </div>
    </div>
  );
}