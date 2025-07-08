import { initialState } from "./store";

export const links = (state = initialState, action) => {
  switch (action.type) {
    case "addLink":
      var arr = Object.assign(state);
      return [...arr, action.payload];
    case "setLinks":
      return action.payload;
    case "updateSelectedLinks":
      var newState = Object.assign(state);
      newState[action.payload.index].selected = action.payload.value;
      return newState;
    default:
      return state;
  }
};

export const addLink = (link) => ({
  type: "addLink",
  payload: link,
});

export const setLinks = (links) => ({
  type: "setLinks",
  payload: links,
});

export const updateSelectedLinks = (newValue) => ({
  type: "updateSelectedLinks",
  payload: newValue,
});
