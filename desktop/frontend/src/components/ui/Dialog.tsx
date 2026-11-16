import { useId, useRef, type ReactNode } from "react";
import { createPortal } from "react-dom";

import { Icon } from "./Icon";
import { useFocusTrap } from "../../hooks/useFocusTrap";
import { useI18n } from "../../i18n/I18nProvider";
import "./Dialog.css";

export function Dialog({
  title,
  onClose,
  children,
  wide = false,
  className,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  wide?: boolean;
  className?: string;
}) {
  const { t } = useI18n();
  const headingID = useId();
  const panelRef = useRef<HTMLDivElement>(null);
  useFocusTrap(panelRef, true, onClose);

  return createPortal(
    <div
      className="dialog-overlay"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <div
        className={[wide ? "dialog dialog--wide" : "dialog", className].filter(Boolean).join(" ")}
        role="dialog"
        aria-modal="true"
        aria-labelledby={headingID}
        ref={panelRef}
        tabIndex={-1}
      >
        <header className="dialog-header">
          <h2 id={headingID}>{title}</h2>
          <button
            type="button"
            className="btn btn-ghost btn-sm dialog-close"
            onClick={onClose}
            aria-label={t("common.close")}
          >
            <Icon name="close" />
          </button>
        </header>
        <div className="dialog-body">{children}</div>
      </div>
    </div>,
    document.body,
  );
}
