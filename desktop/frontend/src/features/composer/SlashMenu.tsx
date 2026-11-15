import type { SlashItem } from "./useSlashData";
import { Icon } from "../../components/ui/Icon";
import { useI18n } from "../../i18n/I18nProvider";
import "./SlashMenu.css";

export function SlashMenu({
  items,
  activeIndex,
  onPick,
  onHover,
}: {
  items: SlashItem[];
  activeIndex: number;
  onPick: (item: SlashItem) => void;
  onHover: (index: number) => void;
}) {
  const { t } = useI18n();
  if (items.length === 0) {
    return null;
  }
  return (
    <div className="slash-menu" role="listbox" aria-label={t("composer.slashTitle")}>
      {items.map((item, index) => (
        <button
          key={`${item.kind}-${item.name}`}
          type="button"
          id={`slash-option-${index}`}
          role="option"
          aria-selected={index === activeIndex}
          className="slash-menu__item"
          data-active={index === activeIndex}
          onMouseEnter={() => onHover(index)}
          onMouseDown={(event) => {
            event.preventDefault();
            onPick(item);
          }}
        >
          <span className="slash-menu__glyph" aria-hidden="true">
            <Icon name={item.kind === "skill" ? "slash" : "sliders"} />
          </span>
          <span className="slash-menu__name mono">{item.name}</span>
          <span className="slash-menu__meta">
            {item.kind === "skill"
              ? item.manual
                ? t("composer.skillManual")
                : t("composer.skillAlways")
              : item.risk}
          </span>
          {item.description ? (
            <span className="slash-menu__description">{item.description}</span>
          ) : null}
        </button>
      ))}
    </div>
  );
}
