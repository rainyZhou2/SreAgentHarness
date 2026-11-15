import logo from "../../assets/logo.png";
import { useI18n } from "../../i18n/I18nProvider";
import type { MessageKey } from "../../i18n/messages";
import { useUiStore } from "../../state/uiStore";
import "./HomeView.css";

const quickActions: Array<{ title: MessageKey; prompt: MessageKey }> = [
  { title: "home.quick1Title", prompt: "home.quick1Prompt" },
  { title: "home.quick2Title", prompt: "home.quick2Prompt" },
  { title: "home.quick3Title", prompt: "home.quick3Prompt" },
  { title: "home.quick4Title", prompt: "home.quick4Prompt" },
];

export function HomeView() {
  const { t } = useI18n();
  const setDraft = useUiStore((state) => state.setDraft);

  function prefill(prompt: string) {
    setDraft("home", prompt);
    window.setTimeout(() => document.getElementById("composer-input")?.focus(), 0);
  }

  return (
    <div className="home-view">
      <img className="home-view__logo" src={logo} alt="" />
      <div className="home-view__intro">
        <h1>{t("home.title")}</h1>
        <p>{t("home.subtitle")}</p>
      </div>
      <div className="home-view__grid">
        {quickActions.map((action) => (
          <button
            key={action.title}
            type="button"
            className="home-card"
            onClick={() => prefill(t(action.prompt))}
          >
            <span className="home-card__title">{t(action.title)}</span>
            <span className="home-card__prompt">{t(action.prompt)}</span>
          </button>
        ))}
      </div>
      <p className="home-view__hint">{t("home.quickHint")}</p>
    </div>
  );
}
